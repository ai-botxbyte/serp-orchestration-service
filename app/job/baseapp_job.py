from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional
from datetime import datetime
from loguru import logger


class BaseAppJob(ABC):
    """Base job class that can be reused by all jobs following baseapp patterns"""
    
    def __init__(self):
        """
        Initialize the base job.
        """
        self.job_name = self.__class__.__name__
        self.start_time: Optional[datetime] = None
        logger.info(f"{self.job_name} initialized")
    
    @abstractmethod
    async def process_message(self,  message: Any) -> None:
        """
        Process a single message - Must be implemented by subclasses
        """

    
    
    async def execute(self,  message: Any) -> None:
        """
        Main execution method with error handling and metrics
        """
        self.start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting {self.job_name} processing")
            await self.process_message(message=message)
            logger.info(f"Successfully completed {self.job_name} processing")
            
        except (ValueError, TypeError, AttributeError, RuntimeError) as e:
            logger.error(f"Job {self.job_name} failed: {e}")
            logger.error(f"Failed message: {message}")
            # Re-raise to let consumer handle dead letter queue
            raise e
            
        finally:
            self.start_time = None
