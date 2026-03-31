"""SERP Orchestration Service - Handles calls to SERP Lambda Service."""

from __future__ import annotations

from typing import Dict, Any, List, Optional
import httpx
from loguru import logger

from app.config.baseapp_config import get_base_config
from app.exception.serp_exception import (
    SerpServiceCallException,
    SerpRequestValidationException,
    SerpResponseException
)


class SerpOrchestrationService:
    """Service for orchestrating SERP search operations."""

    def __init__(self, serp_lambda_url: Optional[str] = None):
        """
        Initialize the SERP orchestration service.

        Args:
            serp_lambda_url: URL of the SERP lambda service.
                           If not provided, defaults to SERP_LAMBDA_SERVICE_URL from config.
        """
        self.config = get_base_config()
        self.serp_lambda_url = serp_lambda_url or getattr(
            self.config, 'SERP_LAMBDA_SERVICE_URL', 'http://localhost:8802'
        )
        self.search_endpoint = f"{self.serp_lambda_url}/api/v1/search"
        # Increased timeout for batch processing (100 queries can take time)
        self.timeout = httpx.Timeout(600.0, connect=60.0)
        logger.info(f"SerpOrchestrationService initialized with URL: {self.serp_lambda_url}")

    def validate_search_request(self, message: dict) -> Dict[str, Any]:
        """
        Validate the search request message.

        Args:
            message: The message containing search request data

        Returns:
            Validated request data

        Raises:
            SerpRequestValidationException: If validation fails
        """
        errors = []

        # Extract data from message
        data = message.get("data", message)

        # Validate queries
        queries = data.get("queries", [])
        if not queries:
            errors.append("Missing required field: queries")
        elif not isinstance(queries, list):
            errors.append("queries must be a list")
        else:
            for idx, query_item in enumerate(queries):
                if not isinstance(query_item, dict):
                    errors.append(f"Query {idx}: must be an object")
                    continue
                if "query" not in query_item:
                    errors.append(f"Query {idx}: missing required field 'query'")
                elif not query_item.get("query", "").strip():
                    errors.append(f"Query {idx}: query cannot be empty")

        # Validate search_type (optional, defaults to google-news)
        search_type = data.get("search_type", "google-news")
        valid_search_types = ["google-news", "google-web", "google-images"]
        if search_type not in valid_search_types:
            errors.append(f"search_type must be one of: {', '.join(valid_search_types)}")

        if errors:
            raise SerpRequestValidationException("; ".join(errors))

        return {
            "queries": queries,
            "batch_id": data.get("batch_id"),
            "search_type": search_type
        }

    async def call_search_api(self, request_data: dict) -> Dict[str, Any]:
        """
        Call the SERP lambda service search API.

        Args:
            request_data: Validated search request data

        Returns:
            Search results from the SERP service

        Raises:
            SerpServiceCallException: If the API call fails
        """
        try:
            logger.info(f"Calling SERP search API: {self.search_endpoint}")
            logger.debug(f"Request data: {request_data}")

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.search_endpoint,
                    json=request_data,
                    headers={"Content-Type": "application/json"}
                )

                if response.status_code >= 400:
                    error_detail = response.text
                    logger.error(
                        f"SERP API error: status={response.status_code}, detail={error_detail}"
                    )
                    raise SerpServiceCallException(
                        service_url=self.search_endpoint,
                        error=f"HTTP {response.status_code}: {error_detail}"
                    )

                result = response.json()
                logger.info("SERP search API call successful")
                return result

        except httpx.TimeoutException as e:
            logger.error(f"SERP API timeout: {e}")
            raise SerpServiceCallException(
                service_url=self.search_endpoint,
                error=f"Request timeout: {e}"
            ) from e
        except httpx.RequestError as e:
            logger.error(f"SERP API request error: {e}")
            raise SerpServiceCallException(
                service_url=self.search_endpoint,
                error=f"Request failed: {e}"
            ) from e
        except Exception as e:
            logger.error(f"SERP API unexpected error: {e}")
            raise SerpServiceCallException(
                service_url=self.search_endpoint,
                error=str(e)
            ) from e

    def _is_query_successful(self, item: dict) -> bool:
        """
        Check if a single query result is successful.

        Args:
            item: Query result item

        Returns:
            True if successful, False otherwise
        """
        if not isinstance(item, dict):
            return True

        # Check for explicit failure indicators
        if item.get("response") is False:
            return False
        if item.get("success") is False:
            return False
        if item.get("error"):
            return False

        return True

    def process_search_response(
        self,
        response: Any,
        original_message: dict
    ) -> Dict[str, Any]:
        """
        Process the search response and prepare it for the response queue.

        Args:
            response: Response from the SERP service
            original_message: Original request message

        Returns:
            Processed response ready for the response queue

        Raises:
            SerpResponseException: If response processing fails
        """
        try:
            # Handle list response (multiple queries)
            if isinstance(response, list):
                # Count successful and failed queries
                successful_items = [item for item in response if self._is_query_successful(item)]
                failed_items = [item for item in response if not self._is_query_successful(item)]

                total = len(response)
                success_count = len(successful_items)
                fail_count = len(failed_items)

                # Batch is successful only if ALL queries succeeded
                batch_success = fail_count == 0

                logger.info(
                    f"Batch results: {success_count}/{total} succeeded, "
                    f"{fail_count}/{total} failed"
                )

                return {
                    "success": batch_success,
                    "data": response,
                    "successful_queries": successful_items,
                    "failed_queries": failed_items,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "total_count": total,
                    "batch_id": original_message.get("batch_id"),
                    "original_request": original_message
                }

            # Handle dict response
            if isinstance(response, dict):
                # Check if response itself indicates failure
                is_success = response.get("success", True)
                if response.get("response") is False:
                    is_success = False
                if response.get("error"):
                    is_success = False

                return {
                    "success": is_success,
                    "data": response.get("data", response),
                    "error": response.get("error"),
                    "batch_id": response.get("batch_id") or original_message.get("batch_id"),
                    "original_request": original_message
                }

            # Unknown response type
            raise SerpResponseException(f"Unexpected response type: {type(response)}")

        except SerpResponseException:
            raise
        except Exception as e:
            logger.error(f"Error processing SERP response: {e}")
            raise SerpResponseException(str(e)) from e

    async def execute_search(self, message: dict) -> Dict[str, Any]:
        """
        Execute a complete search operation.

        Args:
            message: The message containing search request data

        Returns:
            Processed search results

        Raises:
            SerpRequestValidationException: If validation fails
            SerpServiceCallException: If the API call fails
            SerpResponseException: If response processing fails
        """
        # Validate request
        request_data = self.validate_search_request(message)
        logger.info(f"Validated search request with {len(request_data['queries'])} queries")

        # Call the search API
        response = await self.call_search_api(request_data)

        # Process and return the response
        processed_response = self.process_search_response(response, message)
        logger.info(f"Search execution complete. Success: {processed_response.get('success')}")

        return processed_response
