"""Data Orchestration Consumer for job status insertion"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from loguru import logger

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.consumer.baseapp_consumer import BaseAppConsumer
from app.config.baseapp_config import get_base_config
from app.job.data_orchestration_job import DataOrchestrationJob
from app.schema.data_orchestration_schema import DataOrchestrationMessageSchema


class DataOrchestrationConsumer(BaseAppConsumer):
    """Consumer for inserting job status into database"""
    
    def __init__(self):
        config = get_base_config()
        job = DataOrchestrationJob()
        
        super().__init__(
            queue_name="data_orchestration_queue",
            job_processor=job,
            config=config
        )
        
        logger.info(f"{self.__class__.__name__} initialized")
        asyncio.create_task(self.connect())
    
    async def start_consuming(self) -> None:
        """Start consuming messages from data_orchestration_queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        self.consumer_tag = await self.queue.consume(self.send_to_job)
        logger.info(f"Consumer started successfully for queue: {self.queue_name}")
        
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
    async def send_to_job(self, message: AbstractIncomingMessage) -> None:
        """Process data orchestration message"""
        async with message.process():
            try:
                message_data = json.loads(message.body.decode())
                logger.debug(f"Received data orchestration message: {message_data}")
                
                validated_message = self._validate_message(message_data=message_data)
                
                if validated_message is None:
                    logger.warning("Skipping invalid message")
                    return
                
                await self.job_processor.execute(message=validated_message)
                
                logger.info(
                    f"Successfully processed data orchestration message from: "
                    f"{validated_message.source_queue} (completed={validated_message.job_completed})"
                )
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in '{self.queue_name}': {e}")
                await self._send_to_dead_letter_queue(
                    message_body=message.body,
                    error_reason=f"JSON Decode Error: {e}"
                )
                
            except ValueError as e:
                logger.error(f"Validation error in '{self.queue_name}': {e}")
                await self._send_to_dead_letter_queue(
                    message_body=message.body,
                    error_reason=f"Validation Error: {e}"
                )
                
            except (RuntimeError, AttributeError, TypeError) as e:
                logger.error(f"Processing error in '{self.queue_name}': {e}")
                await self._send_to_dead_letter_queue(
                    message_body=message.body,
                    error_reason=f"Processing Error: {e}"
                )
    
    def _validate_message(self, message_data: dict) -> DataOrchestrationMessageSchema:
        """Validate data orchestration message using schema"""
        try:
            data_message = DataOrchestrationMessageSchema(**message_data)
            logger.debug(
                f"Validated data orchestration message: {data_message.source_queue} -> "
                f"{data_message.source_job} (completed={data_message.job_completed})"
            )
            return data_message
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Message validation failed: {e}, Message data: {message_data}")
            return None
    
    async def _send_to_dead_letter_queue(self, message_body: bytes, error_reason: str) -> None:
        """Store failed message in dead letter queue"""
        try:
            dead_letter_message = {
                "original_message": message_body.decode() if isinstance(message_body, bytes) else str(message_body),
                "error_reason": error_reason,
                "failed_at": datetime.utcnow().isoformat(),
                "queue_name": self.queue_name,
                "message_id": f"dlq_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hash(message_body) % 10000}"
            }
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(dead_letter_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={
                        "x-dead-letter-reason": error_reason,
                        "x-failed-at": dead_letter_message["failed_at"],
                        "x-original-queue": self.queue_name
                    }
                ),
                routing_key=f"{self.queue_name}_dead_letter"
            )
            
            logger.error(f"FAILED MESSAGE STORED IN DLQ: {error_reason}")
            
        except (ConnectionError, RuntimeError, AttributeError) as e:
            logger.critical(f"CRITICAL: Failed to store message in DLQ: {e}")

