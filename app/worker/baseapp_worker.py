"""Base worker class for orchestrating consumer and job execution"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Any
import json
from datetime import datetime

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from loguru import logger

from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema
from app.exception.consumer_demo_exception import ConsumerDemoJobException


class BaseAppWorker(ABC):
    """Base worker class that orchestrates consumer validation and job execution"""
    
    def __init__(self, queue_name: str, consumer: Any):
        """
        Initialize the base worker.
        
        Args:
            queue_name: Name of the queue to consume from
            consumer: Consumer instance for message validation and connection management
        """
        self.queue_name = queue_name
        self.consumer = consumer
        self.consumer_tag: Optional[str] = None
        logger.info(f"{self.__class__.__name__} initialized for queue: {queue_name}")
    
    async def connect(self) -> None:
        """Establish connection to RabbitMQ using consumer's connection"""
        # Use consumer's connection
        await self.consumer.connect()
        
        # Declare data_orchestration_queue for job status (RabbitMQ Postgres connector will consume)
        await self.consumer.channel.declare_queue(
            "data_orchestration_queue",
            durable=True,
            auto_delete=False,
            arguments={
                "x-max-priority": 10
            }
        )
        
        logger.info(f"Worker connected to RabbitMQ queue: {self.queue_name}")
    
    async def disconnect(self) -> None:
        """Close RabbitMQ connection using consumer's disconnect"""
        if self.consumer_tag and self.consumer.queue:
            await self.consumer.queue.cancel(self.consumer_tag)
            logger.info("Worker consumer cancelled")
            
        await self.consumer.disconnect()
        logger.info("Worker disconnected from RabbitMQ")
    
    async def start_consuming(self) -> None:
        """Start consuming messages from the queue"""
        if not self.consumer.queue:
            raise RuntimeError("Queue not initialized. Call connect() first.")
        
        logger.info(f"Starting worker to consume messages from: {self.queue_name}")
        
        # Start consuming using consumer's queue
        self.consumer_tag = await self.consumer.queue.consume(self.process_message)
        logger.info(f"Worker started successfully for queue: {self.queue_name}")
        logger.info(f"Consumer tag: {self.consumer_tag}")
        
        # Keep the worker running indefinitely
        try:
            await asyncio.Future()  # Run forever
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    
    async def process_message(self, message: AbstractIncomingMessage) -> None:
        """
        Process a single message: validate, execute jobs, send final response.
        
        This is the main orchestration method that:
        1. Uses consumer to validate message
        2. Initializes and executes related jobs
        3. Tracks completion status
        4. Sends final response to demo_orchestration_queue
        
        Uses try/except with raise for easy debugging.
        """
        async with message.process():
            message_data = None
            trace_id = "unknown"
            
            try:
                # Parse message body
                message_data = json.loads(message.body.decode())
                logger.debug(f"Worker received message: {message_data}")
                trace_id = message_data.get("trace_id", "unknown")
                
                # Use consumer to validate message
                validated_message = self.consumer.validate_message(message_data=message_data)
                
                if validated_message is None:
                    logger.warning(f"[{trace_id}] Worker: Skipping invalid message")
                    await self._send_job_status(
                        trace_id=trace_id,
                        job_name="Unknown",
                        job_completed=False,
                        error_message="Message validation failed"
                    )
                    raise ValueError("Message validation failed")  # Raise for debugging
                
                # Initialize and execute jobs
                try:
                    job_result = await self._execute_jobs(validated_message)
                    
                    # job_result is a tuple (job_completed: bool, job_name: str, job_status: dict)
                    job_completed, job_name, job_status = job_result
                    
                    # Build error message with job failure details
                    error_message = None
                    if not job_completed:
                        failed_count = job_status.get("failed_jobs", 0)
                        failed_names = job_status.get("failed_job_names", [])
                        error_message = (
                            f"{failed_count} job(s) failed: {', '.join(failed_names)}. "
                            f"Total: {job_status.get('total_jobs', 0)}, "
                            f"Successful: {job_status.get('successful_jobs', 0)}, "
                            f"Failed: {failed_count}"
                        )
                    
                    # Send job status to data_orchestration_queue with detailed information
                    await self._send_job_status(
                        trace_id=validated_message.trace_id or trace_id,
                        job_name=job_name,
                        job_completed=job_completed,
                        error_message=error_message,
                        job_status=job_status
                    )
                    
                    logger.info(
                        f"[{trace_id}] Worker completed processing message. "
                        f"Job '{job_name}' - Total: {job_status.get('total_jobs', 0)}, "
                        f"Successful: {job_status.get('successful_jobs', 0)}, "
                        f"Failed: {job_status.get('failed_jobs', 0)}"
                    )
                    
                except ConsumerDemoJobException as job_error:
                    # Job execution failed - raise for debugging
                    logger.error(f"[{trace_id}] Worker: Job execution failed: {job_error}")
                    # Get job name from the exception
                    job_name = job_error.job_name
                    
                    # Create minimal job status for failed execution
                    job_status = {
                        "total_jobs": 1,
                        "successful_jobs": 0,
                        "failed_jobs": 1,
                        "failed_job_names": [job_name],
                        "job_results": [
                            {
                                "job_name": job_name,
                                "success": False,
                                "error_message": str(job_error)
                            }
                        ]
                    }
                    
                    await self._send_job_status(
                        trace_id=validated_message.trace_id or trace_id,
                        job_name=job_name,
                        job_completed=False,
                        error_message=str(job_error),
                        job_status=job_status
                    )
                    raise  # Re-raise for debugging
                except Exception as job_error:
                    # Unexpected error - raise for debugging
                    logger.error(f"[{trace_id}] Worker: Unexpected error: {job_error}")
                    await self._send_job_status(
                        trace_id=validated_message.trace_id or trace_id,
                        job_name="Unknown",
                        job_completed=False,
                        error_message=str(job_error)
                    )
                    raise  # Re-raise for debugging
                
            except json.JSONDecodeError as e:
                logger.error(f"[{trace_id}] Worker JSON decode error in queue '{self.queue_name}': {e}")
                await self._send_job_status(
                    trace_id=trace_id,
                    job_name="Unknown",
                    job_completed=False,
                    error_message=f"JSON Decode Error: {e}"
                )
                raise  # Re-raise for debugging
                
            except Exception as e:
                logger.error(f"[{trace_id}] Worker processing error in queue '{self.queue_name}': {e}")
                await self._send_job_status(
                    trace_id=trace_id,
                    job_name="Unknown",
                    job_completed=False,
                    error_message=str(e)
                )
                raise  # Re-raise for debugging
    
    @abstractmethod
    async def _execute_jobs(self, message: DemoOrchestrationMessageSchema) -> tuple[bool, str, dict]:
        """
        Execute all related jobs for the message.
        
        Args:
            message: Validated message schema
            
        Returns:
            Tuple of (job_completed: bool, job_name: str, job_status: dict)
            - job_completed: True if all jobs completed successfully, False otherwise
            - job_name: Combined name of jobs executed
            - job_status: Dictionary with job execution details:
                - total_jobs: Total number of jobs executed
                - successful_jobs: Number of successful jobs
                - failed_jobs: Number of failed jobs
                - failed_job_names: List of job names that failed
                - job_results: List of job execution results
            
        Raises:
            Exception: If any job fails (for debugging)
        """
        pass
    
    async def _send_job_status(
        self, 
        trace_id: str,
        job_name: str,
        job_completed: bool, 
        error_message: Optional[str] = None,
        job_status: Optional[dict] = None
    ) -> None:
        """
        Send job status to data_orchestration_queue.
        
        RabbitMQ Postgres connector will consume from this queue and insert into database.
        
        Args:
            trace_id: Trace ID for request tracking
            job_name: Name of the job that was executed
            job_completed: True if job completed successfully, False otherwise
            error_message: Error message if job_completed is False
            job_status: Optional dictionary with detailed job execution status:
                - total_jobs: Total number of jobs executed
                - successful_jobs: Number of successful jobs
                - failed_jobs: Number of failed jobs
                - failed_job_names: List of job names that failed
                - job_results: List of job execution results
        """
        try:
            if not self.consumer.channel:
                logger.error(f"[{trace_id}] Consumer channel not available")
                return
            
            metadata = {
                "status_updated_at": datetime.utcnow().isoformat()
            }
            
            # Add job status details to metadata if provided
            if job_status:
                metadata.update({
                    "total_jobs": job_status.get("total_jobs", 0),
                    "successful_jobs": job_status.get("successful_jobs", 0),
                    "failed_jobs": job_status.get("failed_jobs", 0),
                    "failed_job_names": job_status.get("failed_job_names", []),
                    "job_results": job_status.get("job_results", [])
                })
            
            status_message = {
                "source_queue": self.queue_name,
                "source_job": job_name,
                "trace_id": trace_id,
                "job_completed": job_completed,
                "error_message": error_message,
                "metadata": metadata
            }
            
            await self.consumer.channel.default_exchange.publish(
                aio_pika.Message(
                    json.dumps(status_message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=5
                ),
                routing_key="data_orchestration_queue"
            )
            
            logger.info(
                f"[{trace_id}] Job status sent to data_orchestration_queue: "
                f"job='{job_name}', completed={job_completed}"
            )
            
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"[{trace_id}] Failed to send job status: {e}")

