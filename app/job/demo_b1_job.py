"""Demo B1 Job - Name Validation Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema
from app.exception.consumer_demo_exception import JobDemoServiceException
from app.service.demo_b_orchestration_service import DemoBOrchestrationService


class DemoB1Job(BaseAppJob):
    """Job handler for Demo B - Job Type 1 (Name Validation)"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")
    
    async def process_message(self, message: DemoOrchestrationMessageSchema) -> None:
        """Process name validation message."""
        trace_id = message.trace_id or "unknown"
        validation_data = message.data
        
        logger.info(f"[{trace_id}] Starting Demo B1 Job - Name Validation")
        
        try:
            logger.info(f"[{trace_id}] Step 1: Validating name")
            validation = await self._validate_name(validation_data, trace_id)
            
            if not validation["valid"]:
                logger.error(f"[{trace_id}] Name validation failed: {validation['errors']}")
                raise ValueError(f"Name validation failed: {', '.join(validation['errors'])}")
            
            logger.info(f"[{trace_id}] Step 2: Processing name validation")
            await self._process_name_validation(validation_data, trace_id)
            
            logger.info(f"[{trace_id}] ✓ Demo B1 Job completed: Name validation successful")
            
        except ValueError as validation_error:
            logger.error(f"[{trace_id}] Validation error: {validation_error}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoBService",
                service_error=f"Validation failed: {validation_error}"
            ) from validation_error
            
        except Exception as e:
            logger.error(f"[{trace_id}] Demo B1 Job failed: {e}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoBService",
                service_error=str(e)
            ) from e
    
    async def _validate_name(self, validation_data: dict, _trace_id: str) -> dict:
        """Validate name using service."""
        service = DemoBOrchestrationService(db=None)
        return await service.validate_name(validation_data)
    
    async def _process_name_validation(self, validation_data: dict, trace_id: str) -> dict:
        """Process name validation."""
        result = {
            "validated": True,
            "name": validation_data.get("name", ""),
            "validation_timestamp": trace_id
        }
        
        logger.debug(f"[{trace_id}] Name validation processed")
        return result
