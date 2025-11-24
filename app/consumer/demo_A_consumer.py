from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Dict, Optional
from loguru import logger

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.consumer.baseapp_consumer import BaseAppConsumer

from app.config.baseapp_config import get_base_config
from app.job.demo_a1_job import DemoA1Job
from app.job.demo_a2_job import DemoA2Job
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema


class DemoAConsumer(BaseAppConsumer):
    """
    Demo A consumer with multi-job routing support.
    
    This consumer routes messages to different jobs based on job_type:
    - job_type='job1' -> DemoA1Job (User Registration Workflow)
    - job_type='job2' -> DemoA2Job (Order Processing Workflow)
    """
    
    def __init__(self):
        """
        Initialize the demo A consumer with multiple job handlers.
        """
        config = get_base_config()
        
        # Initialize with None - we'll route dynamically
        super().__init__(
            queue_name="demo_A_queue",  # Queue for demo A service
            job_processor=None,  # Will be set dynamically based on job_type
            config=config
        )
        
        # Initialize multiple job handlers
        self.job_handlers: Dict[str, any] = {
            'job1': DemoA1Job(),  # User Registration job
            'job2': DemoA2Job(),  # Order Processing job
        }
        
        logger.info(
            f"{self.__class__.__name__} initialized with {len(self.job_handlers)} job handlers: "
            f"{list(self.job_handlers.keys())}"
        )
        
        # Establish connection during initialization
        asyncio.create_task(self.connect())
    
    async def start_consuming(self) -> None:
        """Start consuming messages from the queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        # Start consuming and get consumer tag
        self.consumer_tag = await self.queue.consume(self.send_to_job)
        logger.info(f"Consumer started successfully for queue: {self.queue_name}")
        logger.info(f"Consumer tag: {self.consumer_tag}")
        
        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
    async def send_to_job(self, message: AbstractIncomingMessage) -> None:
        """
        Process a single message with validation and routing.
        
        Routes messages to appropriate job handler based on job_type field.
        """
        async with message.process():
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.debug(f"Received message: {message_data}")
                
                # Validate message using schema
                validated_message = self._validate_message(message_data=message_data)
                
                # Skip processing if validation failed
                if validated_message is None:
                    logger.warning("Skipping invalid message")
                    return
                
                # Route to appropriate job based on job_type
                job_type = validated_message.job_type
                job_handler = self.job_handlers.get(job_type)
                
                if job_handler is None:
                    logger.error(f"No job handler found for job_type: {job_type}")
                    raise ValueError(f"Invalid job_type: {job_type}")
                
                logger.info(f"Routing to {job_handler.__class__.__name__} for job_type: {job_type}")
                
                # Delegate to appropriate job processor
                job_completed = False
                error_message = None
                
                try:
                    await job_handler.execute(message=validated_message)
                    job_completed = True
                    
                    logger.info(
                        f"Successfully processed message from queue: {self.queue_name} "
                        f"using {job_handler.__class__.__name__}"
                    )
                except Exception as job_error:
                    job_completed = False
                    error_message = str(job_error)
                    logger.error(f"Job execution failed: {job_error}")
                
                # Send response to data_queue (only response, no metadata)
                await self._send_to_data_queue(
                    original_message=validated_message
                )
                
                # Send job status to data_orchestration_queue
                await self._send_to_data_orchestration_queue(
                    job_handler=job_handler.__class__.__name__,
                    trace_id=validated_message.trace_id,
                    job_completed=job_completed,
                    error_message=error_message
                )
                        
            except json.JSONDecodeError as e:
                # Demo A JSON decode error
                logger.error(f"JSON decode error in queue '{self.queue_name}': {e}")
                
                # Send to dead letter queue but don't raise
                await self._send_to_dead_letter_queue(message_body=message.body, error_reason=f"JSON Decode Error: {e}")
                
            except ValueError as e:  # From validation
                # Demo A validation error
                logger.error(f"Validation error in queue '{self.queue_name}': {e}")
                
                # Send to dead letter queue but don't raise
                await self._send_to_dead_letter_queue(message_body=message.body, error_reason=f"Validation Error: {e}")
                
            except (RuntimeError, AttributeError, TypeError) as e:  # From job/service
                # Demo A job processing error
                logger.error(f"Job processing error in queue '{self.queue_name}': {e}")
                
                # Send to dead letter queue but don't raise
                await self._send_to_dead_letter_queue(message_body=message.body, error_reason=f"Job Processing Error: {e}")
                
            # Consumer continues running - no exceptions raised!
    
    def _validate_message(self, message_data: dict) -> DemoOrchestrationMessageSchema:
        """
        Validate log message using DemoOrchestrationMessageSchema with built-in business rules
        """
        try:
            # Validate and parse message using Pydantic schema (includes business rules)
            demo_message = DemoOrchestrationMessageSchema(**message_data)
            print(demo_message,"demo_message")
            
            logger.debug(f"Validated demo A message: {demo_message.data}")
            return demo_message
            
        except (ValueError, TypeError, KeyError) as e:
            # Demo A the error with the raw message data for debugging
            logger.error("Message validation failed. Error: {}, Message data: {}", str(e), message_data)
            # Skip invalid messages instead of raising an error
            return None
    
    async def _send_to_dead_letter_queue(self,  message_body: bytes, error_reason: str) -> None:
        """
        Store failed message permanently in dead letter queue for monitoring and analysis
        """
        try:
            dead_letter_message = {
                "original_message": message_body.decode() if isinstance(message_body, bytes) else str(message_body),
                "error_reason": error_reason,
                "failed_at": datetime.utcnow().isoformat(),
                "queue_name": self.queue_name,
                "service_name": getattr(self, 'service_name', 'unknown'),
                "message_id": f"dlq_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{hash(message_body) % 10000}"
            }
            
            # Store in dead letter queue (permanent storage)
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
            
            # Monitoring and alerting
            logger.error("  FAILED MESSAGE STORED IN DLQ")
            logger.error(f"   Queue: {self.queue_name}")
            logger.error(f"   Error: {error_reason}")
            logger.error(f"   Message ID: {dead_letter_message['message_id']}")
            logger.error(f"   Failed at: {dead_letter_message['failed_at']}")
            
            
            
        except (ConnectionError, RuntimeError, AttributeError) as e:
            logger.critical(f"CRITICAL: Failed to store message in dead letter queue: {e}")
            logger.critical("This message will be permanently lost!")
    
    async def _send_to_data_queue(
        self, original_message: DemoOrchestrationMessageSchema
    ) -> None:
        """Send response to data_queue - only the response data, no metadata"""
        try:
            # Send only the response data from the original message
            response_data = original_message.data
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(response_data).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=5
                ),
                routing_key="data_queue"
            )
            
            logger.debug("Response sent to data_queue")
            
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f"Failed to send to data_queue: {e}")
    
    async def _send_to_data_orchestration_queue(
        self, job_handler: str, trace_id: str, job_completed: bool, error_message: Optional[str] = None
    ) -> None:
        """Send job status to data_orchestration_queue for database insertion"""
        try:
            status_message = {
                "source_queue": self.queue_name,
                "source_job": job_handler,
                "trace_id": trace_id,
                "job_completed": job_completed,
                "error_message": error_message,
                "metadata": {
                    "status_updated_at": datetime.utcnow().isoformat()
                }
            }
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(status_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=5
                ),
                routing_key="data_orchestration_queue"
            )
            
            logger.debug(f"[{trace_id}] Job status sent to data_orchestration_queue: completed={job_completed}")
            
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f"[{trace_id}] Failed to send to data_orchestration_queue: {e}")
