"""SERP Consumer - Batches queries from serp_req_queue and routes to response/DLX queues."""

from __future__ import annotations

from typing import Optional, Any, List, Dict
import json
import asyncio
import uuid
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.job.serp_job import SerpSearchJob
from app.helper.rabbitmq_helper import RabbitMQHelper
from app.exception.serp_exception import SerpJobException


class SerpConsumer:
    """
    SERP Consumer - Batches queries and routes responses.

    Consumes from: serp_req_queue (individual queries)
    Batches up to 100 queries, then calls serp_lambda_service
    On success: publishes to serp_response_queue
    On failure: publishes individual queries to serp_req_dlx_queue
    """

    SERP_REQ_QUEUE = "serp_req_queue"
    SERP_RESPONSE_QUEUE = "serp_response_queue"
    SERP_REQ_DLX_QUEUE = "serp_req_dlx_queue"

    BATCH_SIZE = 100
    BATCH_TIMEOUT_SECONDS = 5  # Max wait time to fill a batch
    DEFAULT_SEARCH_TYPE = "google-web"

    def __init__(self, config: Any, serp_lambda_url: str = None, worker_id: str = None):
        """
        Initialize the SERP consumer.

        Args:
            config: Configuration object
            serp_lambda_url: URL of the SERP lambda service
            worker_id: Unique identifier for this worker instance
        """
        self.config = config
        self.worker_id = worker_id or str(uuid.uuid4())[:8]
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.serp_job = SerpSearchJob(serp_lambda_url=serp_lambda_url)
        self.rabbitmq_helper = RabbitMQHelper()

        # Batch collection (for individual query messages)
        self._batch: List[Dict[str, Any]] = []
        self._batch_messages: List[AbstractIncomingMessage] = []
        self._batch_lock = asyncio.Lock()

        logger.info(f"[{self.worker_id}] {self.__class__.__name__} initialized")
        logger.info(f"[{self.worker_id}] Batch size: {self.BATCH_SIZE}, Timeout: {self.BATCH_TIMEOUT_SECONDS}s")

    async def connect(self) -> None:
        """Establish connection to RabbitMQ and declare all required queues."""
        try:
            self.connection = await aio_pika.connect_robust(
                self.config.RABBITMQ_URL,
                heartbeat=60,
                blocked_connection_timeout=300,
            )
            self.channel = await self.connection.channel()
            # Set prefetch to batch size for fair distribution among workers
            # Each worker will prefetch up to BATCH_SIZE messages
            # This ensures that with 2 workers and 200 messages, each gets ~100
            await self.channel.set_qos(prefetch_count=self.BATCH_SIZE, global_=False)

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

    def _generate_batch_id(self) -> str:
        """Generate a unique batch ID (UUID)."""
        return str(uuid.uuid4())

    def _create_batch_request(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a batch request for the SERP lambda service.

        Args:
            queries: List of query items

        Returns:
            Batch request payload
        """
        return {
            "batch_id": self._generate_batch_id(),
            "search_type": self.DEFAULT_SEARCH_TYPE,
            "queries": queries
        }

    async def _process_batch(self) -> None:
        """Process the current batch of queries."""
        async with self._batch_lock:
            if not self._batch:
                return

            batch_queries = self._batch.copy()
            batch_messages = self._batch_messages.copy()
            self._batch.clear()
            self._batch_messages.clear()

        batch_request = self._create_batch_request(batch_queries)
        batch_id = batch_request["batch_id"]

        # Create mapping of query_id to (query_data, message)
        query_message_map = {
            q["query_id"]: (q, msg)
            for q, msg in zip(batch_queries, batch_messages)
        }

        logger.info(f"[{self.worker_id}] Processing batch {batch_id} with {len(batch_queries)} queries")

        try:
            # Execute the SERP job with the batched request
            result = await self.serp_job.execute(message=batch_request)

            # Check for partial success
            failed_queries = result.get("failed_queries", [])
            successful_queries = result.get("successful_queries", [])
            fail_count = result.get("fail_count", 0)
            success_count = result.get("success_count", 0)

            if result.get("success") and fail_count == 0:
                # All queries succeeded - send one by one to response queue
                await self._send_successful_to_response_individually(
                    successful_queries=successful_queries,
                    query_message_map=query_message_map,
                    batch_id=batch_id
                )
                logger.info(f"[{self.worker_id}] ✓ Batch {batch_id} fully successful ({success_count} queries)")

            elif fail_count > 0 and success_count > 0:
                # Partial success
                logger.info(
                    f"[{self.worker_id}] ⚠ Batch {batch_id}: {success_count} succeeded, {fail_count} failed"
                )

                # Send successful to response queue one by one
                if successful_queries:
                    await self._send_successful_to_response_individually(
                        successful_queries=successful_queries,
                        query_message_map=query_message_map,
                        batch_id=batch_id
                    )

                # Send failed queries to DLX one by one
                if failed_queries:
                    await self._send_failed_to_dlx_individually(
                        failed_queries=failed_queries,
                        query_message_map=query_message_map,
                        batch_id=batch_id
                    )

            else:
                # All failed
                logger.warning(f"✗ Batch {batch_id} completely failed ({fail_count} queries)")
                await self._send_failed_to_dlx_individually(
                    failed_queries=failed_queries if failed_queries else [
                        {"query": q["query"], "query_id": q["query_id"], "error": "All queries failed"}
                        for q in batch_queries
                    ],
                    query_message_map=query_message_map,
                    batch_id=batch_id
                )

        except SerpJobException as e:
            logger.error(f"SERP job failed for batch {batch_id}: {e}")
            await self._send_all_to_dlx(batch_queries, batch_messages, batch_id, str(e))

        except Exception as e:
            logger.error(f"Unexpected error for batch {batch_id}: {e}")
            await self._send_all_to_dlx(batch_queries, batch_messages, batch_id, str(e))

    async def _send_successful_to_response_individually(
        self,
        successful_queries: List[Dict],
        query_message_map: Dict[str, tuple],
        batch_id: str
    ) -> None:
        """
        Send successful queries to response queue one by one.

        Args:
            successful_queries: List of successful query results
            query_message_map: Mapping of query_id to (query_data, message)
            batch_id: The batch ID
        """
        for sq in successful_queries:
            query_id = sq.get("query_id")

            # Create individual response message
            response_message = {
                "success": True,
                "query": sq.get("query"),
                "query_id": query_id,
                "response": sq.get("response"),
                "data": sq.get("data"),
                "batch_id": batch_id
            }

            published = await self._publish_to_queue(
                queue_name=self.SERP_RESPONSE_QUEUE,
                message=response_message,
                priority=5
            )

            if query_id and query_id in query_message_map:
                _, msg = query_message_map[query_id]
                if published:
                    await msg.ack()
                else:
                    await msg.nack(requeue=True)

        logger.info(f"✓ {len(successful_queries)} successful queries sent to response queue individually")

    async def _send_failed_to_dlx_individually(
        self,
        failed_queries: List[Dict],
        query_message_map: Dict[str, tuple],
        batch_id: str
    ) -> None:
        """
        Send failed queries to DLX one by one.

        Args:
            failed_queries: List of failed query results
            query_message_map: Mapping of query_id to (query_data, message)
            batch_id: The batch ID
        """
        for fq in failed_queries:
            query_id = fq.get("query_id")
            error = fq.get("error", "Unknown error")

            if query_id and query_id in query_message_map:
                original_query, msg = query_message_map[query_id]
                retry_count = original_query.get("_retry_count", 0)

                dlx_message = {
                    "query": fq.get("query") or original_query.get("query"),
                    "query_id": query_id,
                    "_retry_count": retry_count + 1,
                    "_last_error": error,
                    "_failed_batch_id": batch_id
                }

                published = await self._publish_to_queue(
                    queue_name=self.SERP_REQ_DLX_QUEUE,
                    message=dlx_message,
                    priority=3
                )

                if published:
                    await msg.ack()
                else:
                    await msg.nack(requeue=True)

        logger.info(f"✗ {len(failed_queries)} failed queries sent to DLX individually")

    async def _send_all_to_dlx(
        self,
        queries: List[Dict],
        messages: List[AbstractIncomingMessage],
        batch_id: str,
        error: str
    ) -> None:
        """Send all queries to DLX one by one."""
        for query_data, msg in zip(queries, messages):
            retry_count = query_data.get("_retry_count", 0)
            dlx_message = {
                "query": query_data.get("query"),
                "query_id": query_data.get("query_id"),
                "_retry_count": retry_count + 1,
                "_last_error": error,
                "_failed_batch_id": batch_id
            }

            published = await self._publish_to_queue(
                queue_name=self.SERP_REQ_DLX_QUEUE,
                message=dlx_message,
                priority=3
            )

            if published:
                await msg.ack()
            else:
                await msg.nack(requeue=True)

        logger.info(f"✗ {len(queries)} queries sent to DLX individually")

    async def _collect_message(self, message: AbstractIncomingMessage) -> None:
        """
        Collect incoming message into the batch.

        Args:
            message: Incoming RabbitMQ message
        """
        try:
            message_data = json.loads(message.body.decode())

            # Validate message has required fields
            if "query" not in message_data:
                logger.warning(f"Message missing 'query' field, discarding")
                await message.ack()
                return

            # Extract query item
            query_item = {
                "query": message_data.get("query"),
                "query_id": message_data.get("query_id", str(uuid.uuid4()))
            }

            # Preserve retry metadata if present
            if "_retry_count" in message_data:
                query_item["_retry_count"] = message_data["_retry_count"]

            async with self._batch_lock:
                self._batch.append(query_item)
                self._batch_messages.append(message)
                current_batch_size = len(self._batch)

            logger.debug(
                f"[{self.worker_id}] Collected query {query_item['query_id']}, "
                f"batch size: {current_batch_size}/{self.BATCH_SIZE}"
            )

            # Process batch if full
            if current_batch_size >= self.BATCH_SIZE:
                logger.info(f"[{self.worker_id}] Batch full ({self.BATCH_SIZE} queries), processing...")
                await self._process_batch()

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            await message.ack()

        except Exception as e:
            logger.error(f"Error collecting message: {e}")
            await message.nack(requeue=True)

    async def _batch_timeout_handler(self) -> None:
        """Periodically process incomplete batches after timeout."""
        while True:
            await asyncio.sleep(self.BATCH_TIMEOUT_SECONDS)

            async with self._batch_lock:
                batch_size = len(self._batch)

            if batch_size > 0:
                logger.info(f"[{self.worker_id}] Batch timeout, processing {batch_size} queries")
                await self._process_batch()

    async def start_consuming(self) -> None:
        """Start consuming messages from serp_req_queue."""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")

        logger.info(f"[{self.worker_id}] Starting to consume from: {self.SERP_REQ_QUEUE}")
        logger.info(f"[{self.worker_id}] Batching: {self.BATCH_SIZE} queries, timeout: {self.BATCH_TIMEOUT_SECONDS}s")

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
