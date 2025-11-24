"""Demo A1 Job - Social Validation Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema
from app.exception.consumer_demo_exception import JobDemoServiceException
from app.service.demo_a_orchestration_service import DemoAOrchestrationService


class DemoA1Job(BaseAppJob):
    """Job handler for Demo A - Job Type 1 (Social Validation)"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")
    
    async def process_message(self, message: DemoOrchestrationMessageSchema) -> None:
        """Process social validation message."""
        trace_id = message.trace_id or "unknown"
        validation_data = message.data
        
        logger.info(f"[{trace_id}] Starting Demo A1 Job - Social Validation")
        
        try:
            logger.info(f"[{trace_id}] Step 1: Validating social accounts")
            validation = await self._validate_social_accounts(validation_data, trace_id)
            
            if not validation["valid"]:
                logger.error(f"[{trace_id}] Social validation failed: {validation['errors']}")
                raise ValueError(f"Social validation failed: {', '.join(validation['errors'])}")
            
            logger.info(f"[{trace_id}] Step 2: Processing social accounts")
            await self._process_social_validation(validation_data, trace_id)
            
            logger.info(f"[{trace_id}] ✓ Demo A1 Job completed: Social validation successful")
            
        except ValueError as validation_error:
            logger.error(f"[{trace_id}] Validation error: {validation_error}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=f"Validation failed: {validation_error}"
            ) from validation_error
            
        except Exception as e:
            logger.error(f"[{trace_id}] Demo A1 Job failed: {e}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=str(e)
            ) from e
    
    async def _validate_social_accounts(self, validation_data: dict, _trace_id: str) -> dict:
        """Validate social accounts using service."""
        service = DemoAOrchestrationService(db=None)
        return await service.validate_social_accounts(validation_data)
    
    async def _process_social_validation(self, validation_data: dict, trace_id: str) -> dict:
        """Process social validation."""
        result = {
            "validated": True,
            "social_accounts": validation_data.get("social_accounts", []),
            "validation_timestamp": trace_id
        }
        
        logger.debug(f"[{trace_id}] Social validation processed")
        return result
