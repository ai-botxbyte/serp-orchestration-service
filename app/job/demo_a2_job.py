"""Demo A2 Job - Auto Tagging Workflow"""

from __future__ import annotations
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.schema.demo_orchestration_schema import DemoOrchestrationMessageSchema
from app.exception.consumer_demo_exception import JobDemoServiceException


class DemoA2Job(BaseAppJob):
    """Job handler for Demo A - Job Type 2 (Auto Tagging)"""
    
    def __init__(self):
        super().__init__()
        logger.info(f"{self.__class__.__name__} initialized")
    
    async def process_message(self, message: DemoOrchestrationMessageSchema) -> None:
        """Process auto tagging message."""
        trace_id = message.trace_id or "unknown"
        tagging_data = message.data
        
        logger.info(f"[{trace_id}] Starting Demo A2 Job - Auto Tagging")
        
        try:
            logger.info(f"[{trace_id}] Step 1: Analyzing content for tags")
            analysis = await self._analyze_content(tagging_data, trace_id)
            
            logger.info(f"[{trace_id}] Step 2: Generating tags")
            tags = await self._generate_tags(analysis, trace_id)
            
            logger.info(f"[{trace_id}] Step 3: Applying tags")
            await self._apply_tags(tagging_data, tags, trace_id)
            
            logger.info(f"[{trace_id}] ✓ Demo A2 Job completed: {len(tags)} tags applied")
            
        except ValueError as validation_error:
            logger.error(f"[{trace_id}] Validation error: {validation_error}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=f"Validation failed: {validation_error}"
            ) from validation_error
            
        except Exception as e:
            logger.error(f"[{trace_id}] Demo A2 Job failed: {e}")
            raise JobDemoServiceException(
                job_name=self.__class__.__name__,
                service_name="DemoAService",
                service_error=str(e)
            ) from e
    
    async def _analyze_content(self, tagging_data: dict, trace_id: str) -> dict:
        """Analyze content for auto tagging."""
        analysis = {
            "name": tagging_data.get("name", ""),
            "description": tagging_data.get("description", ""),
            "keywords": []
        }
        
        logger.debug(f"[{trace_id}] Content analyzed")
        return analysis
    
    async def _generate_tags(self, analysis: dict, trace_id: str) -> list:
        """Generate tags based on analysis."""
        tags = []
        
        if analysis.get("name"):
            tags.append("demo")
        if analysis.get("description"):
            tags.append("tagged")
        
        logger.debug(f"[{trace_id}] Generated {len(tags)} tags")
        return tags
    
    async def _apply_tags(self, _tagging_data: dict, tags: list, trace_id: str) -> dict:
        """Apply generated tags."""
        result = {
            "tags": tags,
            "applied_at": trace_id
        }
        
        logger.debug(f"[{trace_id}] Tags applied")
        return result
