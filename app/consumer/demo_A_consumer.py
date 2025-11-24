from __future__ import annotations

import asyncio
import json
from loguru import logger

from aio_pika.abc import AbstractIncomingMessage

from app.consumer.baseapp_consumer import BaseAppConsumer
from app.config.baseapp_config import get_base_config
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema


class DemoAConsumer(BaseAppConsumer):
    """
    Demo A consumer - only consumes and validates messages.
    
    Job execution is handled by DemoAWorker.
    """
    
    def __init__(self):
        """
        Initialize the demo A consumer for message validation only.
        """
        config = get_base_config()
        
        # Initialize with None - consumer only validates, doesn't process jobs
        super().__init__(
            queue_name="demo_A_queue",  # Queue for demo A service
            job_processor=None,  # No job processor - validation only
            config=config
        )
        
        logger.info(f"{self.__class__.__name__} initialized for message validation only")
        
        # Establish connection during initialization
        asyncio.create_task(self.connect())
    
    async def start_consuming(self) -> None:
        """Start consuming messages from demo_A_queue"""
        if not self.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting to consume messages from: {self.queue_name}")
        
        # Start consuming and get consumer tag
        self.consumer_tag = await self.queue.consume(self.process_message)
        logger.info(f"Consumer started successfully for queue: {self.queue_name}")
        logger.info(f"Consumer tag: {self.consumer_tag}")
        
        # Keep the consumer running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
    async def process_message(self, message: AbstractIncomingMessage) -> None:
        """
        Process message: consume and validate.
        Job execution is handled by workers.
        """
        async with message.process():
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.debug(f"Consumer received message: {message_data}")
                
                # Validate message using schema
                validated_message = self.validate_message(message_data=message_data)
                
                if validated_message is None:
                    logger.warning("Consumer: Skipping invalid message")
                    return
                
                logger.info("Consumer: Message validated successfully")
                # Note: Job execution is handled by workers, not consumers
                        
            except json.JSONDecodeError as e:
                logger.error(f"Consumer JSON decode error in queue '{self.queue_name}': {e}")
                
            except ValueError as e:  # From validation
                logger.error(f"Consumer validation error in queue '{self.queue_name}': {e}")
    
    def validate_message(self, message_data: dict) -> DemoOrchestrationMessageSchema:
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
    
