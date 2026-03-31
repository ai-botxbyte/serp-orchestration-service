"""SERP DLX Consumer - Consumes from serp_req_dlx_queue and retries to serp_req_queue.

This consumer implements infinite retry logic - messages are continuously
retried until they succeed. NO delays - immediate republishing.
"""

from __future__ import annotations

from typing import Optional, Any, List, Dict
import json
import asyncio
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.helper.rabbitmq_helper import RabbitMQHelper


class SerpDLXConsumer:
    """
    SERP DLX Consumer - Handles failed individual query messages and retries.

    Consumes from: serp_req_dlx_queue (individual queries)
    Retries to: serp_req_queue (individual queries)

    IMMEDIATE processing - no delays. Collects available messages and
    republishes them right away.
    """

    SERP_REQ_QUEUE = "serp_req_queue"
    SERP_REQ_DLX_QUEUE = "serp_req_dlx_queue"

    # Batch settings - short timeout for quick collection
    BATCH_SIZE = 100
    BATCH_TIMEOUT_SECONDS = 1  # Very short - just collect what's immediately available

    def __init__(self, config: Any):
        """
        Initialize the SERP DLX consumer.

        Args:
            config: Configuration object
        """
        self.config = config
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.rabbitmq_helper = RabbitMQHelper()

        # Batch collection
        self._batch: List[Dict[str, Any]] = []
        self._batch_messages: List[AbstractIncomingMessage] = []
        self._batch_lock = asyncio.Lock()
        self._processing = False

        logger.info(f"{self.__class__.__name__} initialized (IMMEDIATE mode - no delays)")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ and declare all required queues."""
        try:
            self.connection = await aio_pika.connect_robust(
                self.config.RABBITMQ_URL,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            self.channel = await self.connection.channel()
            # Process multiple messages for efficiency
            await self.channel.set_qos(prefetch_count=self.BATCH_SIZE)

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
        # Process any remaining batch before disconnecting
        if self._batch:
            logger.info(f"Processing remaining {len(self._batch)} queries before shutdown")
            await self._process_batch()

        try:
            await self.rabbitmq_helper.close()
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ helper: {e}")

        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    async def _collect_message(self, message: AbstractIncomingMessage) -> None:
        """
        Collect incoming message into the batch.

        Args:
            message: Incoming RabbitMQ message
        """
        try:
            message_data = json.loads(message.body.decode())

            async with self._batch_lock:
                self._batch.append(message_data)
                self._batch_messages.append(message)
                current_batch_size = len(self._batch)

            logger.debug(
                f"DLX collected query {message_data.get('query_id', 'unknown')}, "
                f"batch size: {current_batch_size}/{self.BATCH_SIZE}"
            )

            # Process batch if full
            if current_batch_size >= self.BATCH_SIZE:
                logger.info(f"DLX batch full ({self.BATCH_SIZE} queries), processing immediately...")
                await self._process_batch()

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in DLX: {e}")
            await message.ack()

        except Exception as e:
            logger.error(f"Error collecting DLX message: {e}")
            await message.nack(requeue=True)

    async def _process_batch(self) -> None:
        """Process the current batch - IMMEDIATELY republish all without any delay."""
        async with self._batch_lock:
            if not self._batch or self._processing:
                return

            self._processing = True
            batch_data = self._batch.copy()
            batch_messages = self._batch_messages.copy()
            self._batch.clear()
            self._batch_messages.clear()

        try:
            if not batch_data:
                return

            logger.info(f"DLX republishing {len(batch_data)} queries IMMEDIATELY")

            # Republish all messages in parallel - NO DELAY
            publish_tasks = []
            for message_data, original_msg in zip(batch_data, batch_messages):
                retry_count = message_data.get("_retry_count", 1)

                retry_message = {
                    "query": message_data.get("query"),
                    "query_id": message_data.get("query_id"),
                    "_retry_count": retry_count,
                    "_last_error": message_data.get("_last_error", "Unknown")
                }

                publish_tasks.append(
                    self._republish_and_ack(retry_message, original_msg, retry_count)
                )

            # Execute all republishes concurrently
            await asyncio.gather(*publish_tasks)

            logger.info(f"↻ DLX republished {len(batch_data)} queries to {self.SERP_REQ_QUEUE}")

        except Exception as e:
            logger.error(f"Error processing DLX batch: {e}")
            # Nack all messages for requeue
            for msg in batch_messages:
                try:
                    await msg.nack(requeue=True)
                except Exception:
                    pass
        finally:
            self._processing = False

    async def _republish_and_ack(
        self,
        retry_message: Dict[str, Any],
        original_msg: AbstractIncomingMessage,
        retry_count: int
    ) -> None:
        """Republish a single message and ack the original."""
        try:
            published = await self.rabbitmq_helper.publish_message(
                queue_name=self.SERP_REQ_QUEUE,
                message=retry_message,
                priority=3,  # Lower priority for retries
                ensure_queue=True
            )

            if published:
                await original_msg.ack()
                logger.debug(
                    f"↻ Query {retry_message.get('query_id')} republished (retry #{retry_count + 1})"
                )
            else:
                await original_msg.nack(requeue=True)
                logger.error(f"Failed to republish query {retry_message.get('query_id')}")

        except Exception as e:
            logger.error(f"Error republishing query {retry_message.get('query_id')}: {e}")
            await original_msg.nack(requeue=True)

    async def _batch_timeout_handler(self) -> None:
        """Periodically process incomplete batches after timeout."""
        while True:
            await asyncio.sleep(self.BATCH_TIMEOUT_SECONDS)

            async with self._batch_lock:
                batch_size = len(self._batch)

            if batch_size > 0 and not self._processing:
                logger.info(f"DLX timeout, processing {batch_size} queries immediately")
                await self._process_batch()

    async def start_consuming(self) -> None:
        """Start consuming messages from serp_req_dlx_queue."""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")

        logger.info(f"Starting to consume from: {self.SERP_REQ_DLX_QUEUE}")
        logger.info("Mode: IMMEDIATE RETRY (no delays)")

        # Start the timeout handler
        timeout_task = asyncio.create_task(self._batch_timeout_handler())

        await self.queue.consume(self._collect_message, no_ack=False)

        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            timeout_task.cancel()
            try:
                await timeout_task
            except asyncio.CancelledError:
                pass
