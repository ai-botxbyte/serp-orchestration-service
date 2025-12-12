"""Demo B Worker - Orchestrates consumer validation and job execution"""

from __future__ import annotations

from loguru import logger

from app.worker.baseapp_worker import BaseAppWorker
from app.consumer.demo_B_consumer import DemoBConsumer
from app.job.demo_b1_job import DemoB1Job
from app.exception.consumer_demo_exception import ConsumerDemoJobException


class DemoBWorker(BaseAppWorker):
    """
    Demo B Worker that orchestrates consumer validation and job execution.
    
    Flow:
    1. Consumes messages from demo_B_queue
    2. Uses DemoBConsumer to validate messages
    3. Initializes and executes related jobs based on job_type
    """
    
    def __init__(self):
        """Initialize Demo B Worker with consumer and job handlers"""
        # Create consumer for validation only
        consumer = DemoBConsumer()
        
        super().__init__(
            queue_name="demo_B_queue",
            consumer=consumer
        )
        
        # Initialize job handlers
        self.job_handlers: dict[str, any] = {
            'job1': DemoB1Job(),  # Name Validation job
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

