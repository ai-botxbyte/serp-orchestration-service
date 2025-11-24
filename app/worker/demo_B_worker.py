"""Demo B Worker - Orchestrates consumer validation and job execution"""

from __future__ import annotations

from typing import Dict, Tuple
from loguru import logger

from app.worker.baseapp_worker import BaseAppWorker
from app.consumer.demo_B_consumer import DemoBConsumer
from app.job.demo_b1_job import DemoB1Job
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema
from app.exception.consumer_demo_exception import ConsumerDemoJobException


class DemoBWorker(BaseAppWorker):
    """
    Demo B Worker that orchestrates consumer validation and job execution.
    
    Flow:
    1. Consumes messages from demo_B_queue
    2. Uses DemoBConsumer to validate messages
    3. Initializes and executes related jobs based on job_type
    4. Tracks completion status
    5. Sends final response (true/false) to demo_orchestration_queue
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
        self.job_handlers: Dict[str, any] = {
            'job1': DemoB1Job(),  # Name Validation job
        }
        
        logger.info(
            f"{self.__class__.__name__} initialized with {len(self.job_handlers)} job handlers: "
            f"{list(self.job_handlers.keys())}"
        )
    
    async def _execute_jobs(self, message: DemoOrchestrationMessageSchema) -> Tuple[bool, str, dict]:
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
                - job_results: List of tuples (job_name, success, error_message)
            
        Raises:
            ConsumerDemoJobException: If any job fails (for debugging)
        """
        trace_id = message.trace_id or "unknown"
        job_type = message.job_type
        
        logger.info(f"[{trace_id}] Worker: Starting job execution for job_type: {job_type}")
        
        # Get appropriate job handler
        job_handler = self.job_handlers.get(job_type)
        
        if job_handler is None:
            error_msg = f"No job handler found for job_type: {job_type}"
            logger.error(f"[{trace_id}] {error_msg}")
            raise ConsumerDemoJobException(
                queue_name=self.queue_name,
                job_name="Unknown",
                job_error=error_msg
            )
        
        job_name = job_handler.__class__.__name__
        logger.info(f"[{trace_id}] Worker: Routing to {job_name}")
        
        # Track job execution results
        job_results = []
        
        # Execute job with error handling
        try:
            await job_handler.execute(message=message)
            logger.info(
                f"[{trace_id}] Worker: Job {job_name} completed successfully"
            )
            job_results.append((job_name, True, None))  # type: ignore
            
        except Exception as job_error:
            # Track failure
            error_msg = str(job_error)
            logger.error(
                f"[{trace_id}] Worker: Job {job_name} failed: {error_msg}"
            )
            job_results.append((job_name, False, error_msg))
            
            # Raise exception for debugging - worker will catch and send job status
            raise ConsumerDemoJobException(
                queue_name=self.queue_name,
                job_name=job_name,
                job_error=error_msg
            ) from job_error
        
        # Calculate job status summary
        total_jobs = len(job_results)
        successful_jobs = sum(1 for _, success, _ in job_results if success)
        failed_jobs = total_jobs - successful_jobs
        failed_job_names = [name for name, success, _ in job_results if not success]
        all_completed = failed_jobs == 0
        
        job_status = {
            "total_jobs": total_jobs,
            "successful_jobs": successful_jobs,
            "failed_jobs": failed_jobs,
            "failed_job_names": failed_job_names,
            "job_results": [
                {
                    "job_name": name,
                    "success": success,
                    "error_message": error_msg
                }
                for name, success, error_msg in job_results
            ]
        }
        
        # Create combined job name for status reporting
        combined_job_name = job_name if total_jobs == 1 else f"Multiple jobs ({total_jobs})"
        
        return (all_completed, combined_job_name, job_status)

