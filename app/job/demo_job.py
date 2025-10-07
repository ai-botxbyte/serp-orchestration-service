from __future__ import annotations

from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.config.database import get_async_db
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema
from app.exception.consumer_demo_exception import JobDemoServiceException

class DemoJob(BaseAppJob):
    """Job that handles business logic and orchestration for processing demo messages"""
    
    def __init__(self):
        """
        Initialize the demo processing job.
        """
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized as orchestrator")
    
    async def process_message(self,  message: DemoOrchestrationMessageSchema) -> None:
        """
        Process demo message by orchestrating service calls
        Job acts as orchestrator handling both business logic and infrastructure
        """
        
        # Get database session (orchestration responsibility)
        async for session in get_async_db():
            try:
                # await self._store_log_message(session=session, log_message=message)
                # await session.commit()  # Like this we can store the message in the database
                logger.info(f"Successfully stored demo message from service: {message.data}")
                
            except Exception as e:
                # Handle all errors
                await session.rollback()
                logger.error(f"Error processing demo from {message.data}: {e}")
                raise JobDemoServiceException(
                    job_name=self.__class__.__name__,
                    service_name="DemoService",
                    service_error=str(e)
                ) from e 
            finally:
                await session.close()
    
