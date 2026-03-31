"""SERP DLX Consumer - Consumes from serp_req_dlx_queue and retries to serp_req_queue.

This consumer implements infinite retry logic - messages are continuously
retried until they succeed.
"""

from __future__ import annotations

from typing import Optional, Any
import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.helper.rabbitmq_helper import RabbitMQHelper


class SerpDLXConsumer:
    """
    SERP DLX Consumer - Handles failed messages and retries.

    Consumes from: serp_req_dlx_queue
    Retries to: serp_req_queue

    Implements infinite retry with configurable delay.
    """

    SERP_REQ_QUEUE = "serp_req_queue"
    SERP_REQ_DLX_QUEUE = "serp_req_dlx_queue"

    def __init__(
        self,
        config: Any,
        retry_delay_seconds: int = 30,
        max_retry_delay_seconds: int = 300
    ):
        """
        Initialize the SERP DLX consumer.

        Args:
            config: Configuration object
            retry_delay_seconds: Initial delay between retries (default: 30 seconds)
            max_retry_delay_seconds: Maximum delay between retries (default: 300 seconds / 5 minutes)
        """
        self.config = config
        self.retry_delay_seconds = retry_delay_seconds
        self.max_retry_delay_seconds = max_retry_delay_seconds
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.rabbitmq_helper = RabbitMQHelper()
        logger.info(f"{self.__class__.__name__} initialized")
        logger.info(f"Retry delay: {retry_delay_seconds}s, Max delay: {max_retry_delay_seconds}s")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ and declare all required queues."""
        try:
            self.connection = await aio_pika.connect_robust(
                self.config.RABBITMQ_URL,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            # Declare the DLX queue (consume from)
            self.queue = await self.channel.declare_queue(
                self.SERP_REQ_DLX_QUEUE,
                durable=True,
                auto_delete=False,
                arguments={"x-max-priority": 10}
            )

            # Declare the main request queue (retry to)
            await self.channel.declare_queue(
                self.SERP_REQ_QUEUE,
                durable=True,
                auto_delete=False,
                arguments={"x-max-priority": 10}
            )

            logger.info(f"Connected to RabbitMQ. Consuming from: {self.SERP_REQ_DLX_QUEUE}")
            logger.info(f"Retry queue: {self.SERP_REQ_QUEUE}")

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def disconnect(self) -> None:
        """Close RabbitMQ connection."""
        try:
            await self.rabbitmq_helper.close()
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ helper: {e}")

        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    def _calculate_retry_delay(self, retry_count: int) -> int:
        """
        Calculate retry delay using exponential backoff.

        Args:
            retry_count: Current retry count

        Returns:
            Delay in seconds (capped at max_retry_delay_seconds)
        """
        # Exponential backoff: delay = base_delay * 2^(retry_count - 1)
        # Capped at max_retry_delay_seconds
        delay = min(
            self.retry_delay_seconds * (2 ** min(retry_count - 1, 5)),
            self.max_retry_delay_seconds
        )
        return delay

    async def _consume_message(self, message: AbstractIncomingMessage) -> None:
        """
        Handle incoming message from serp_req_dlx_queue.

        Waits for delay and then republishes to serp_req_queue.
        Implements infinite retry - never gives up.
        """
        async with message.process():
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                retry_count = message_data.get("_retry_count", 1)
                last_error = message_data.get("_last_error", "Unknown")
                batch_id = message_data.get("batch_id") or message_data.get("data", {}).get("batch_id", "no-batch-id")

                logger.info(
                    f"DLX Consumer received message (retry #{retry_count}): {batch_id}"
                )
                logger.debug(f"Last error: {last_error}")

                # Calculate and apply retry delay
                delay = self._calculate_retry_delay(retry_count)
                logger.info(f"Waiting {delay} seconds before retry #{retry_count + 1}...")
                await asyncio.sleep(delay)

                # Prepare message for retry (keep retry metadata)
                retry_message = {
                    **message_data,
                    "_retry_count": retry_count,
                    "_last_error": last_error
                }

                # Publish back to request queue
                published = await self.rabbitmq_helper.publish_message(
                    queue_name=self.SERP_REQ_QUEUE,
                    message=retry_message,
                    priority=3,  # Lower priority for retries
                    ensure_queue=True
                )

                if published:
                    logger.info(
                        f"↻ Message republished to {self.SERP_REQ_QUEUE} "
                        f"(retry #{retry_count + 1}): {batch_id}"
                    )
                else:
                    logger.error(
                        f"Failed to republish to {self.SERP_REQ_QUEUE}, "
                        f"message stays in DLX queue"
                    )
                    # Re-publish to DLX for another attempt
                    await self.rabbitmq_helper.publish_message(
                        queue_name=self.SERP_REQ_DLX_QUEUE,
                        message=retry_message,
                        priority=3,
                        ensure_queue=True
                    )

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in DLX consumer: {e}")
                # Discard malformed messages
                logger.warning("Discarding malformed message")

            except Exception as e:
                logger.error(f"Unexpected error in DLX consumer: {e}")
                # Try to re-queue the message
                try:
                    message_data = json.loads(message.body.decode())
                    await self.rabbitmq_helper.publish_message(
                        queue_name=self.SERP_REQ_DLX_QUEUE,
                        message=message_data,
                        priority=3,
                        ensure_queue=True
                    )
                except Exception as requeue_error:
                    logger.error(f"Failed to requeue message: {requeue_error}")

    async def start_consuming(self) -> None:
        """Start consuming messages from serp_req_dlx_queue."""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")

        logger.info(f"Starting to consume messages from: {self.SERP_REQ_DLX_QUEUE}")
        logger.info("Mode: INFINITE RETRY (will retry until success)")

        # Start consuming
        await self.queue.consume(self._consume_message)

        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
