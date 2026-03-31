"""SERP Job - Handles search request processing."""

from __future__ import annotations

from typing import Dict, Any
from loguru import logger

from app.job.baseapp_job import BaseAppJob
from app.service.serp_orchestration_service import SerpOrchestrationService
from app.exception.serp_exception import (
    SerpJobException,
    SerpServiceCallException,
    SerpRequestValidationException,
    SerpResponseException
)


class SerpSearchJob(BaseAppJob):
    """Job handler for SERP search operations."""

    def __init__(self, serp_lambda_url: str = None):
        """
        Initialize the SERP search job.

        Args:
            serp_lambda_url: URL of the SERP lambda service
        """
        super().__init__()
        self.serp_service = SerpOrchestrationService(serp_lambda_url=serp_lambda_url)
        self.result: Dict[str, Any] = {}
        logger.info(f"{self.__class__.__name__} initialized")

    async def execute(self, message: dict) -> Dict[str, Any]:
        """
        Execute the SERP search job.

        Args:
            message: Message containing search request data

        Returns:
            Search results with success status

        Raises:
            SerpJobException: If the job execution fails
        """
        logger.info(f"Starting {self.__class__.__name__}")

        try:
            # Execute the search
            result = await self.serp_service.execute_search(message)

            # Store the result
            self.result = result

            if result.get("success"):
                logger.info(f"✓ {self.__class__.__name__} completed successfully")
            else:
                logger.warning(
                    f"⚠ {self.__class__.__name__} completed with errors: {result.get('error')}"
                )

            return result

        except SerpRequestValidationException as e:
            logger.error(f"Validation error in {self.__class__.__name__}: {e}")
            raise SerpJobException(
                job_name=self.__class__.__name__,
                error=f"Validation failed: {e.message}"
            ) from e

        except SerpServiceCallException as e:
            logger.error(f"Service call error in {self.__class__.__name__}: {e}")
            raise SerpJobException(
                job_name=self.__class__.__name__,
                error=f"Service call failed: {e.message}"
            ) from e

        except SerpResponseException as e:
            logger.error(f"Response processing error in {self.__class__.__name__}: {e}")
            raise SerpJobException(
                job_name=self.__class__.__name__,
                error=f"Response processing failed: {e.message}"
            ) from e

        except Exception as e:
            logger.error(f"Unexpected error in {self.__class__.__name__}: {e}")
            raise SerpJobException(
                job_name=self.__class__.__name__,
                error=str(e)
            ) from e

    def get_result(self) -> Dict[str, Any]:
        """
        Get the result of the last job execution.

        Returns:
            The result from the last execution
        """
        return self.result
