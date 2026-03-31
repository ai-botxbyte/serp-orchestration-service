"""SERP Consumer - Consumes from serp_req_queue and routes to response/DLX queues."""

from __future__ import annotations

from typing import Optional, Any
import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.job.serp_job import SerpSearchJob
from app.helper.rabbitmq_helper import RabbitMQHelper
from app.exception.serp_exception import SerpJobException


class SerpConsumer:
    """
    SERP Consumer - Consumes search requests and routes responses.

    Consumes from: serp_req_queue
    On success: publishes to serp_response_queue
    On failure: publishes to serp_req_dlx_queue
    """

    SERP_REQ_QUEUE = "serp_req_queue"
    SERP_RESPONSE_QUEUE = "serp_response_queue"
    SERP_REQ_DLX_QUEUE = "serp_req_dlx_queue"

    def __init__(self, config: Any, serp_lambda_url: str = None):
        """
        Initialize the SERP consumer.

        Args:
            config: Configuration object
            serp_lambda_url: URL of the SERP lambda service
        """
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.serp_job = SerpSearchJob(serp_lambda_url=serp_lambda_url)
        self.rabbitmq_helper = RabbitMQHelper()
        logger.info(f"{self.__class__.__name__} initialized")

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

            # Declare the main request queue
            self.queue = await self.channel.declare_queue(
                self.SERP_REQ_QUEUE,
                durable=True,
                auto_delete=False,
                arguments={"x-max-priority": 10}
            )

            # Declare the response queue
            await self.channel.declare_queue(
                self.SERP_RESPONSE_QUEUE,
                durable=True,
                auto_delete=False,
                arguments={"x-max-priority": 10}
            )

            # Declare the DLX queue
            await self.channel.declare_queue(
                self.SERP_REQ_DLX_QUEUE,
                durable=True,
                auto_delete=False,
                arguments={"x-max-priority": 10}
            )

            logger.info(f"Connected to RabbitMQ. Consuming from: {self.SERP_REQ_QUEUE}")
            logger.info(f"Response queue: {self.SERP_RESPONSE_QUEUE}")
            logger.info(f"DLX queue: {self.SERP_REQ_DLX_QUEUE}")

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

    async def _publish_to_queue(
        self,
        queue_name: str,
        message: dict,
        priority: int = 5
    ) -> bool:
        """
        Publish a message to a specified queue.

        Args:
            queue_name: Target queue name
            message: Message to publish
            priority: Message priority

        Returns:
            True if published successfully
        """
        try:
            return await self.rabbitmq_helper.publish_message(
                queue_name=queue_name,
                message=message,
                priority=priority,
                ensure_queue=True
            )
        except Exception as e:
            logger.error(f"Failed to publish to {queue_name}: {e}")
            return False

    async def _consume_message(self, message: AbstractIncomingMessage) -> None:
        """
        Handle incoming message from serp_req_queue.

        On success: sends result to serp_response_queue
        On failure: sends original message to serp_req_dlx_queue
        """
        async with message.process():
            message_data = None
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.info(f"SERP Consumer received message: {message_data.get('batch_id', 'no-batch-id')}")
                logger.debug(f"Full message: {message_data}")

                # Execute the SERP job
                result = await self.serp_job.execute(message=message_data)

                # Check if execution was successful
                if result.get("success"):
                    # Publish to response queue
                    published = await self._publish_to_queue(
                        queue_name=self.SERP_RESPONSE_QUEUE,
                        message=result,
                        priority=5
                    )
                    if published:
                        logger.info(
                            f"✓ Message processed successfully, sent to {self.SERP_RESPONSE_QUEUE}"
                        )
                    else:
                        logger.error(f"Failed to publish success response to {self.SERP_RESPONSE_QUEUE}")
                        # Send to DLX if we can't publish to response
                        await self._send_to_dlx(message_data, "Failed to publish to response queue")
                else:
                    # Result indicates failure
                    logger.warning(f"SERP job returned failure: {result.get('error')}")
                    await self._send_to_dlx(message_data, result.get('error', 'Unknown error'))

            except SerpJobException as e:
                logger.error(f"SERP job execution failed: {e}")
                if message_data:
                    await self._send_to_dlx(message_data, str(e))

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                # Cannot send to DLX without valid message data
                # Message will be acknowledged to prevent infinite loop

            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}")
                if message_data:
                    await self._send_to_dlx(message_data, str(e))

    async def _send_to_dlx(self, message_data: dict, error: str) -> None:
        """
        Send failed message to DLX queue.

        Args:
            message_data: Original message data
            error: Error description
        """
        # Add retry metadata
        retry_count = message_data.get("_retry_count", 0)
        dlx_message = {
            **message_data,
            "_retry_count": retry_count + 1,
            "_last_error": error,
            "_original_queue": self.SERP_REQ_QUEUE
        }

        published = await self._publish_to_queue(
            queue_name=self.SERP_REQ_DLX_QUEUE,
            message=dlx_message,
            priority=3
        )

        if published:
            logger.info(
                f"✗ Message sent to DLX queue (retry #{retry_count + 1}): {error}"
            )
        else:
            logger.error(f"Failed to publish to DLX queue: {self.SERP_REQ_DLX_QUEUE}")

    async def start_consuming(self) -> None:
        """Start consuming messages from serp_req_queue."""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")

        logger.info(f"Starting to consume messages from: {self.SERP_REQ_QUEUE}")

        # Start consuming
        await self.queue.consume(self._consume_message)

        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
