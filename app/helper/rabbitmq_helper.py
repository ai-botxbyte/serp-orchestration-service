"""
Universal RabbitMQ Helper for Queue Management and Message Publishing

This helper provides a reusable interface for managing RabbitMQ connections,
queue operations, and message publishing. It can be easily copied to other
projects by just configuring queue names in the config file.

Usage:
    from app.helper.rabbitmq_helper import RabbitMQHelper

    # Initialize helper
    rabbitmq_helper = RabbitMQHelper()

    # Ensure queue exists (creates if doesn't exist)
    await rabbitmq_helper.ensure_queue_exists("my_queue_name")

    # Publish a message
    await rabbitmq_helper.publish_message(
        queue_name="my_queue",
        message={"key": "value"}
    )

    # Close connection when done
    await rabbitmq_helper.close()
"""

from __future__ import annotations
import asyncio
import json
import sys
from typing import Optional, Dict, Any, Union
from urllib.parse import quote

import aio_pika
import httpx
from aio_pika import Connection, Channel, Queue, DeliveryMode, Exchange, ExchangeType
from aio_pika.exceptions import AMQPException

from app.config.baseapp_config import get_base_config
from app.config.logger_config import logger


class RabbitMQHelper:
    """
    Universal RabbitMQ helper for managing connections and queues.

    This class provides a reusable interface for RabbitMQ operations that can
    be easily ported to other projects by just updating the config.
    """

    def __init__(self, rabbitmq_url: Optional[str] = None):
        """
        Initialize RabbitMQ helper.

        Args:
            rabbitmq_url: Optional RabbitMQ URL. If not provided, uses config.RABBITMQ_URL

        Note:
            If IS_RABBITMQ_ENABLED=False for this service, the helper will be created
            but all operations will be disabled. Methods will raise ConnectionError
            when RabbitMQ is disabled for this service.
        """
        self.config = get_base_config()
        self._enabled = self.config.IS_RABBITMQ_ENABLED

        if self._enabled:
            self.rabbitmq_url = rabbitmq_url or self.config.RABBITMQ_URL
        else:
            self.rabbitmq_url = None
            logger.warning(
                "RabbitMQ helper initialized but RabbitMQ is disabled for this service "
                "(IS_RABBITMQ_ENABLED=False)"
            )

        self._connection: Optional[Connection] = None
        self._channel: Optional[Channel] = None
        self._queues: Dict[str, Queue] = {}

    async def _ensure_connection(self) -> Connection:
        """
        Ensure RabbitMQ connection exists and is open.

        Returns:
            Active RabbitMQ connection

        Raises:
            ConnectionError: If connection cannot be established or RabbitMQ
                            is disabled for this service
        """
        if not self._enabled:
            raise ConnectionError(
                "RabbitMQ is disabled for this service. "
                "Set IS_RABBITMQ_ENABLED=True to use RabbitMQ operations."
            )

        if self._connection is None or self._connection.is_closed:
            try:
                self._connection = await aio_pika.connect_robust(self.rabbitmq_url)
                logger.info("RabbitMQ connection established")
            except (AMQPException, ConnectionError) as e:
                error_msg = f"Failed to connect to RabbitMQ: {e}"
                logger.error(error_msg)
                print(error_msg, file=sys.stderr)
                self._connection = None
                raise ConnectionError(error_msg) from e

        return self._connection

    async def _ensure_channel(self) -> Channel:
        """
        Ensure RabbitMQ channel exists and is open.

        Returns:
            Active RabbitMQ channel

        Raises:
            ConnectionError: If connection or channel cannot be established
        """
        await self._ensure_connection()

        if self._channel is None or self._channel.is_closed:
            try:
                self._channel = await self._connection.channel()
                logger.debug("RabbitMQ channel created")
            except (AMQPException, ConnectionError) as e:
                error_msg = f"Failed to create RabbitMQ channel: {e}"
                logger.error(error_msg)
                print(error_msg, file=sys.stderr)
                self._channel = None
                raise ConnectionError(error_msg) from e

        return self._channel

    async def ensure_queue_exists(
        self,
        queue_name: str,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> Queue:
        """
        Ensure a queue exists, connecting to existing queue if available, creating it if necessary.

        By default, queues are created as DURABLE (permanent storage) to ensure
        they survive RabbitMQ broker restarts. This is the recommended setting
        for production use.

        This method first checks if the queue already exists in RabbitMQ. If it does,
        it connects directly to the existing queue. If not, it creates a new queue
        with the specified parameters.

        Args:
            queue_name: Name of the queue
            durable: If True, queue survives broker restart
                    (default: True for permanent storage)
            exclusive: If True, queue is only accessible by the current connection (default: False)
            auto_delete: If True, queue is deleted when no longer used
                        (default: False for permanent storage)
            arguments: Optional queue arguments

        Returns:
            Queue object

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If queue creation fails or queue exists with incompatible arguments
        """
        # Check if queue already exists in cache
        if queue_name in self._queues:
            queue = self._queues[queue_name]
            if not queue.channel.is_closed:
                logger.debug(f"Using cached queue '{queue_name}'")
                return queue

        try:
            channel = await self._ensure_channel()

            # First, try to connect to existing queue (passive=True means don't create, just check)
            try:
                existing_queue = await channel.declare_queue(queue_name, passive=True)
                # Queue exists, use it directly
                self._queues[queue_name] = existing_queue
                logger.info(
                    f"Connected to existing queue '{queue_name}' "
                    f"(durable={existing_queue.durable}, "
                    f"exclusive={existing_queue.exclusive}, "
                    f"auto_delete={existing_queue.auto_delete}, "
                    f"arguments={existing_queue.arguments})"
                )
                return existing_queue
            except aio_pika.exceptions.ChannelNotFoundEntity:
                # Queue doesn't exist, will create it below
                # Channel is closed after passive declaration fails, need to recreate it
                self._channel = None
                channel = await self._ensure_channel()

            # Queue doesn't exist, create it with specified parameters
            queue = await channel.declare_queue(
                queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )

            # Cache the queue
            self._queues[queue_name] = queue

            logger.info(
                f"Created new queue '{queue_name}' (durable={durable}, "
                f"exclusive={exclusive}, auto_delete={auto_delete}, arguments={arguments})"
            )

            return queue

        except AMQPException as e:
            error_str = str(e)
            # Handle case where queue exists with different arguments
            if "PRECONDITION_FAILED" in error_str and "inequivalent arg" in error_str:
                # Try to get existing queue info to provide better error message
                try:
                    queue_info = await self.get_queue_info(queue_name)
                    if queue_info:
                        error_msg = (
                            f"Queue '{queue_name}' already exists with different arguments. "
                            f"Existing queue arguments: {queue_info.get('arguments', {})}. "
                            f"Requested arguments: {arguments or {}}. Please delete the existing "
                            f"queue or use matching arguments."
                        )
                        logger.error(error_msg)
                        raise AMQPException(error_msg) from e
                except AMQPException:
                    pass  # Fall through to original error

            error_msg = f"Failed to ensure queue '{queue_name}': {e}"
            logger.error(error_msg)
            raise AMQPException(error_msg) from e
        except ConnectionError as e:
            error_msg = f"Failed to ensure queue '{queue_name}': {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e

    async def delete_queue(
        self, queue_name: str, if_unused: bool = False, if_empty: bool = False
    ) -> None:
        """
        Delete a queue.

        Args:
            queue_name: Name of the queue to delete
            if_unused: Only delete if queue has no consumers
            if_empty: Only delete if queue is empty

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If queue deletion fails
        """
        try:
            channel = await self._ensure_channel()
            await channel.queue_delete(
                queue_name, if_unused=if_unused, if_empty=if_empty
            )

            # Remove from cache
            if queue_name in self._queues:
                del self._queues[queue_name]

            logger.info(f"Queue '{queue_name}' deleted")

        except (AMQPException, ConnectionError) as e:
            error_msg = f"Failed to delete queue '{queue_name}': {e}"
            logger.error(error_msg)
            raise AMQPException(error_msg) from e

    async def get_queue_info(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a queue.

        Args:
            queue_name: Name of the queue

        Returns:
            Dictionary with queue information or None if queue doesn't exist

        Raises:
            ConnectionError: If connection cannot be established
        """
        try:
            channel = await self._ensure_channel()
            queue = await channel.declare_queue(queue_name, passive=True)

            return {
                "name": queue.name,
                "durable": queue.durable,
                "exclusive": queue.exclusive,
                "auto_delete": queue.auto_delete,
                "arguments": queue.arguments,
            }

        except AMQPException:
            # Queue doesn't exist
            return None
        except ConnectionError as e:
            logger.error(f"Failed to get queue info for '{queue_name}': {e}")
            raise

    async def get_queue_details(self, queue_name: str) -> Optional[Dict[str, Any]]:  # pylint: disable=too-many-locals
        """
        Get detailed queue information from RabbitMQ Management API.

        This provides runtime information including consumers, messages, and rates.

        Args:
            queue_name: Name of the queue

        Returns:
            Dictionary with detailed queue information including:
            - consumers: List of consumer details
            - consumer_count: Number of active consumers
            - messages_ready: Number of messages ready to be delivered (pending messages)
            - messages_unacknowledged: Number of messages delivered but not yet acknowledged
            - message_stats: Message statistics including ack, deliver, publish rates and counts

        Raises:
            ConnectionError: If connection to management API fails
            HTTPException: If queue doesn't exist or other API error
        """
        if not self._enabled:
            raise ConnectionError(
                "RabbitMQ is disabled for this service. "
                "Set IS_RABBITMQ_ENABLED=True to use RabbitMQ operations."
            )

        management_url = self.config.RABBITMQ_MANAGEMENT_URL

        # Extract vhost from RABBITMQ_URL (default is "/")
        # RABBITMQ_URL format: amqp://user:pass@host:port/vhost
        vhost = "/"
        if self.rabbitmq_url and "/" in self.rabbitmq_url.split("@")[-1]:
            vhost_part = self.rabbitmq_url.split("@")[-1].split("/", 1)
            if len(vhost_part) > 1 and vhost_part[1]:
                vhost = vhost_part[1]

        # URL encode the queue name and vhost
        encoded_queue_name = quote(queue_name, safe="")
        encoded_vhost = quote(vhost, safe="")

        api_url = f"{management_url}/api/queues/{encoded_vhost}/{encoded_queue_name}"

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(api_url)

                if response.status_code == 404:
                    logger.warning(f"Queue '{queue_name}' not found in RabbitMQ")
                    return None

                response.raise_for_status()
                queue_data = response.json()

                # Extract relevant information
                # Note: RabbitMQ API returns 'consumers' as integer count
                # and 'consumer_details' as the actual list of consumers (or 0 if none)
                consumers_data = queue_data.get("consumer_details", [])
                consumer_count = queue_data.get("consumers", 0)

                # Ensure consumers is always a list (RabbitMQ may return 0 instead of [])
                if isinstance(consumers_data, list):
                    consumers_list = consumers_data
                else:
                    consumers_list = []

                result = {
                    "name": queue_data.get("name"),
                    "vhost": queue_data.get("vhost"),
                    "durable": queue_data.get("durable"),
                    "auto_delete": queue_data.get("auto_delete"),
                    "consumers": consumers_list,
                    "consumer_count": consumer_count
                    if isinstance(consumer_count, int)
                    else 0,
                    "messages": queue_data.get("messages", 0),
                    "messages_ready": queue_data.get("messages_ready", 0),
                    "messages_unacknowledged": queue_data.get(
                        "messages_unacknowledged", 0
                    ),
                    "message_stats": queue_data.get("message_stats", {}),
                    "state": queue_data.get("state"),
                    "node": queue_data.get("node"),
                }

                logger.debug(f"Retrieved detailed info for queue '{queue_name}'")
                return result

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error for '{queue_name}': {e.response.status_code}"
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        except httpx.RequestError as e:
            error_msg = f"Failed to connect to RabbitMQ Management API: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        except Exception as e:
            error_msg = (
                f"Unexpected error getting queue details for '{queue_name}': {e}"
            )
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e

    def is_connected(self) -> bool:
        """
        Check if RabbitMQ connection is active.

        Returns:
            True if connected, False otherwise
        """
        return self._connection is not None and not self._connection.is_closed

    async def _close_channel(self):
        if self._channel and not self._channel.is_closed:
            try:
                await self._channel.close()
                await asyncio.sleep(0.2)
            except (AMQPException, asyncio.TimeoutError) as e:
                logger.warning(f"Error closing channel: {e}")
            finally:
                self._channel = None
                logger.debug("RabbitMQ channel closed")

    async def _close_connection(self):
        if self._connection and not self._connection.is_closed:
            try:
                await self._connection.close()
                await asyncio.sleep(0.2)
            except (AMQPException, asyncio.TimeoutError) as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None
                logger.info("RabbitMQ connection closed")

    async def close(self) -> None:
        """
        Close RabbitMQ connection and channel.

        This should be called when the helper is no longer needed,
        typically during application shutdown.
        """
        try:
            self._queues.clear()
            await self._close_channel()
            await self._close_connection()
        except (AMQPException, asyncio.TimeoutError) as e:
            logger.warning(f"Error during RabbitMQ cleanup: {e}")

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_connection()
        return self

    async def initialize_queues_from_config(self) -> Dict[str, Queue]:
        """
        Initialize queues from config.RABBITMQ_QUEUE_NAMES.

        Parses comma-separated queue names from config and ensures they all exist.
        All queues are created as DURABLE (permanent storage) to survive broker restarts.

        Returns:
            Dictionary mapping queue names to Queue objects

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If queue creation fails
        """
        queue_names_str = self.config.RABBITMQ_QUEUE_NAMES or ""
        queue_names = [
            name.strip() for name in queue_names_str.split(",") if name.strip()
        ]

        initialized_queues = {}

        for queue_name in queue_names:
            # Always create queues as durable (permanent storage) and non-auto-delete
            # Set x-max-priority for log_queue to support message priorities
            queue_args = {}
            if queue_name == "log_queue":
                queue_args = {
                    "x-max-priority": 10
                }  # Support message priorities up to 10

            queue = await self.ensure_queue_exists(
                queue_name=queue_name,
                durable=True,  # Permanent storage - survives broker restart
                exclusive=False,  # Accessible by multiple connections
                auto_delete=False,  # Permanent - not deleted when unused
                arguments=queue_args if queue_args else None,
            )
            initialized_queues[queue_name] = queue

        logger.info(
            f"Initialized {len(initialized_queues)} durable queues from config: "
            f"{', '.join(initialized_queues.keys())}"
        )

        return initialized_queues

    async def _create_aio_message(
        self,
        message: Union[Dict[str, Any], str],
        delivery_mode: DeliveryMode,
        priority: int,
        headers: Optional[Dict[str, Any]],
    ) -> aio_pika.Message:
        if isinstance(message, dict):
            body = json.dumps(message).encode("utf-8")
        elif isinstance(message, str):
            body = message.encode("utf-8")
        else:
            body = json.dumps(message).encode("utf-8")

        return aio_pika.Message(
            body=body,
            delivery_mode=delivery_mode,
            priority=priority,
            headers=headers or {},
        )

    async def _publish_to_exchange(
        self,
        channel: aio_pika.Channel,
        aio_message: aio_pika.Message,
        queue_name: str,
        exchange: Optional[str],
        routing_key: Optional[str],
    ):
        if exchange:
            exchange_obj = await channel.get_exchange(exchange)
            await exchange_obj.publish(
                aio_message, routing_key=routing_key or queue_name
            )
        else:
            await channel.default_exchange.publish(
                aio_message, routing_key=routing_key or queue_name
            )

    async def publish_message(
        self,
        queue_name: str,
        message: Union[Dict[str, Any], str],
        priority: int = 3,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        exchange: Optional[str] = None,
        routing_key: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        ensure_queue: bool = True,
        queue_durable: bool = True,
    ) -> bool:
        """
        Publish a message to a RabbitMQ queue.

        By default, messages are published as PERSISTENT (permanent storage) to ensure
        they survive RabbitMQ broker restarts. Queues are also created as DURABLE by default.
        This is the recommended setting for production use.

        Args:
            queue_name: Name of the queue to publish to
            message: Message to publish (dict or string). If dict, will be JSON-encoded
            priority: Message priority (0-255, higher = more important)
            delivery_mode: Message delivery mode (default: PERSISTENT for permanent storage)
            exchange: Optional exchange name. If None, uses default exchange
            routing_key: Optional routing key. If None, uses queue_name
            headers: Optional message headers
            ensure_queue: If True, ensures queue exists before publishing (default: True)
            queue_durable: If True, queue will be durable/permanent (default: True)

        Returns:
            True if message was published successfully, False otherwise

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If publishing fails
        """
        try:
            if ensure_queue:
                await self.ensure_queue_exists(
                    queue_name=queue_name,
                    durable=queue_durable,
                    exclusive=False,
                    auto_delete=False,
                )

            channel = await self._ensure_channel()
            aio_message = await self._create_aio_message(
                message, delivery_mode, priority, headers
            )
            await self._publish_to_exchange(
                channel, aio_message, queue_name, exchange, routing_key
            )

            logger.debug(
                f"Message published to queue '{queue_name}' (priority={priority}, "
                f"size={len(aio_message.body)} bytes)"
            )
            return True

        except (AMQPException, ConnectionError, json.JSONDecodeError) as e:  # pylint: disable=no-member
            error_msg = f"Failed to publish message to queue '{queue_name}': {e}"
            logger.error(error_msg)
            print(error_msg, file=sys.stderr)
            return False

    async def publish_batch(
        self,
        queue_name: str,
        messages: list[Union[Dict[str, Any], str]],
        priority: int = 3,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        ensure_queue: bool = True,
        queue_durable: bool = True,
    ) -> int:
        """
        Publish multiple messages to a RabbitMQ queue in a batch.

        By default, messages are published as PERSISTENT (permanent storage) and
        queues are created as DURABLE to ensure they survive broker restarts.

        Args:
            queue_name: Name of the queue to publish to
            messages: List of messages to publish
            priority: Message priority for all messages (0-255)
            delivery_mode: Message delivery mode (default: PERSISTENT for permanent storage)
            ensure_queue: If True, ensures queue exists before publishing (default: True)
            queue_durable: If True, queue will be durable/permanent (default: True)

        Returns:
            Number of messages successfully published
        """
        published_count = 0

        for message in messages:
            success = await self.publish_message(
                queue_name=queue_name,
                message=message,
                priority=priority,
                delivery_mode=delivery_mode,
                ensure_queue=ensure_queue
                if published_count == 0
                else False,  # Only ensure once
                queue_durable=queue_durable,
            )

            if success:
                published_count += 1

        logger.info(
            f"Published {published_count}/{len(messages)} messages to queue '{queue_name}'"
        )
        return published_count

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def ensure_exchange_exists(
        self,
        exchange_name: str,
        exchange_type: str = "fanout",
        durable: bool = True,
        auto_delete: bool = False,
    ) -> Exchange:
        """
        Ensure an exchange exists, creating it if necessary.

        Args:
            exchange_name: Name of the exchange
            exchange_type: Type of exchange (fanout, direct, topic, headers)
            durable: If True, exchange survives broker restart
            auto_delete: If True, exchange is deleted when no longer used

        Returns:
            Exchange object

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If exchange creation fails
        """
        try:
            channel = await self._ensure_channel()

            # Map string exchange type to ExchangeType enum
            exchange_type_map = {
                "fanout": ExchangeType.FANOUT,
                "direct": ExchangeType.DIRECT,
                "topic": ExchangeType.TOPIC,
                "headers": ExchangeType.HEADERS,
            }

            exchange_type_enum = exchange_type_map.get(
                exchange_type.lower(), ExchangeType.FANOUT
            )

            exchange = await channel.declare_exchange(
                exchange_name,
                exchange_type_enum,
                durable=durable,
                auto_delete=auto_delete,
            )

            logger.info(
                f"Exchange '{exchange_name}' ensured (type={exchange_type}, durable={durable})"
            )
            return exchange

        except (AMQPException, ConnectionError) as e:
            error_msg = f"Failed to ensure exchange '{exchange_name}': {e}"
            logger.error(error_msg)
            raise AMQPException(error_msg) from e

    async def bind_queue_to_exchange(
        self, queue_name: str, exchange_name: str, routing_key: str = ""
    ) -> None:
        """
        Bind a queue to an exchange.

        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the exchange to bind to
            routing_key: Routing key for the binding (default: "")

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If binding fails
        """
        try:
            await self._ensure_channel()

            # Ensure queue exists
            queue = await self.ensure_queue_exists(queue_name)

            # Bind queue to exchange
            await queue.bind(exchange_name, routing_key=routing_key)

            logger.info(
                f"Queue '{queue_name}' bound to exchange '{exchange_name}' "
                f"with routing_key '{routing_key}'"
            )

        except (AMQPException, ConnectionError) as e:
            error_msg = f"Failed to bind queue '{queue_name}' to exchange '{exchange_name}': {e}"
            logger.error(error_msg)
            raise AMQPException(error_msg) from e

    async def publish_to_exchange(
        self,
        exchange_name: str,
        message: Union[Dict[str, Any], str],
        routing_key: str = "",
        priority: int = 3,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        headers: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish a message to an exchange.

        Args:
            exchange_name: Name of the exchange to publish to
            message: Message to publish (dict or string)
            routing_key: Routing key for the message
            priority: Message priority (0-255)
            delivery_mode: Message delivery mode (default: PERSISTENT)
            headers: Optional message headers

        Returns:
            True if message was published successfully, False otherwise

        Raises:
            ConnectionError: If connection cannot be established
            AMQPException: If publishing fails
        """
        try:
            channel = await self._ensure_channel()

            # Create message
            aio_message = await self._create_aio_message(
                message, delivery_mode, priority, headers
            )

            # Get exchange and publish
            exchange = await channel.get_exchange(exchange_name)
            await exchange.publish(aio_message, routing_key=routing_key)

            logger.debug(
                f"Message published to exchange '{exchange_name}' "
                f"with routing_key '{routing_key}'"
            )
            return True

        except (AMQPException, ConnectionError, json.JSONDecodeError) as e:  # pylint: disable=no-member
            error_msg = f"Failed to publish message to exchange '{exchange_name}': {e}"
            logger.error(error_msg)
            print(error_msg, file=sys.stderr)
            return False
