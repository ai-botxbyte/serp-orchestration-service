"""Data Orchestration Job - Job Status Insertion Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.config.database import get_async_db
from app.schema.data_orchestration_schema import DataOrchestrationMessageSchema
from app.exception.consumer_demo_exception import JobDemoServiceException
from app.service.data_orchestration_service import DataOrchestrationService


class DataOrchestrationJob(BaseAppJob):
    """Job for inserting job status into database"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")
    
    async def process_message(self, message: DataOrchestrationMessageSchema) -> None:
        """Process data orchestration message."""
        trace_id = message.trace_id or "unknown"
        
        logger.info(f"[{trace_id}] Starting Data Orchestration Job: {message.source_queue} -> {message.source_job}")
        
        async for session in get_async_db():
            try:
                logger.info(f"[{trace_id}] Step 1: Validating status message")
                validation = await self._validate_message(session, message, trace_id)
                
                if not validation["valid"]:
                    logger.error(f"[{trace_id}] Validation failed: {validation['errors']}")
                    return
                
                logger.info(f"[{trace_id}] Step 2: Inserting job status")
                inserted = await self._insert_status(session, message, trace_id)
                
                if not inserted:
                    logger.error(f"[{trace_id}] Status insertion failed")
                    return
                
                logger.info(f"[{trace_id}] ✓ Data Orchestration Job completed: {inserted.get('id')}")
                
            except ValueError as validation_error:
                await session.rollback()
                logger.error(f"[{trace_id}] Validation error: {validation_error}")
                raise JobDemoServiceException(
                    job_name=self.__class__.__name__,
                    service_name="DataOrchestrationService",
                    service_error=f"Validation failed: {validation_error}"
                ) from validation_error
                
            except Exception as e:
                await session.rollback()
                logger.error(f"[{trace_id}] Data Orchestration Job failed: {e}")
                raise JobDemoServiceException(
                    job_name=self.__class__.__name__,
                    service_name="DataOrchestrationService",
                    service_error=str(e)
                ) from e
                
            finally:
                await session.close()
    
    async def _validate_message(self, _session, message: DataOrchestrationMessageSchema, _trace_id: str) -> dict:
        """Validate status message using service."""
        service = DataOrchestrationService(db=_session)
        return await service.validate_status_message(message.model_dump())
    
    async def _insert_status(self, session, message: DataOrchestrationMessageSchema, _trace_id: str) -> dict:
        """Insert status using service."""
        service = DataOrchestrationService(db=session)
        return await service.insert_job_status(
            source_queue=message.source_queue,
            source_job=message.source_job,
            job_completed=message.job_completed,
            trace_id=message.trace_id,
            error_message=message.error_message,
            metadata=message.metadata
        )

