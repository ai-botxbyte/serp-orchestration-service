"""Demo A Worker - Orchestrates consumer validation and job execution"""

from __future__ import annotations

from loguru import logger

from app.worker.baseapp_worker import BaseAppWorker
from app.consumer.demo_A_consumer import DemoAConsumer
from app.job.demo_a1_job import DemoA1Job
from app.job.demo_a2_job import DemoA2Job
from app.exception.consumer_demo_exception import ConsumerDemoJobException


class DemoAWorker(BaseAppWorker):
    """
    Demo A Worker that orchestrates consumer validation and job execution.
    
    Flow:
    1. Consumes messages from demo_A_queue
    2. Uses DemoAConsumer to validate messages
    3. Initializes and executes related jobs based on job_type
    """
    
    def __init__(self):
        """Initialize Demo A Worker with consumer and job handlers"""
        # Create consumer for validation only
        consumer = DemoAConsumer()
        
        super().__init__(
            queue_name="demo_A_queue",
            consumer=consumer
        )
        
        # Initialize job handlers
        self.job_handlers: dict[str, any] = {
            'job1': DemoA1Job(),  # User Registration job
            'job2': DemoA2Job(),  # Order Processing job
        }
        
        logger.info(
            f"{self.__class__.__name__} initialized with {len(self.job_handlers)} job handlers: "
            f"{list(self.job_handlers.keys())}"
        )
    
    async def _execute_jobs(self, message: dict) -> None:
        """
        Execute all related jobs for the message.
        
        Args:
            message: Message data dictionary
            
        Raises:
            ConsumerDemoJobException: If any job fails (for debugging)
        """
        job_type = message.get("job_type")
        
        logger.info(f"Worker: Starting job execution for job_type: {job_type}")
        
        # Get appropriate job handler
        job_handler = self.job_handlers.get(job_type)
        
        if job_handler is None:
            error_msg = f"No job handler found for job_type: {job_type}"
            logger.error(error_msg)
            raise ConsumerDemoJobException(
                queue_name=self.queue_name,
                job_name="Unknown",
                job_error=error_msg
            )
        
        job_name = job_handler.__class__.__name__
        logger.info(f"Worker: Routing to {job_name}")
        
        # Execute job
        await job_handler.execute(message=message)
        logger.info(f"Worker: Job {job_name} completed successfully")