import uuid
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
# from app.config.logger_config import get_logger_context


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add correlation ID to requests for tracing purposes.
    
    This middleware:
    1. Generates a unique correlation ID for each request
    2. Adds it to request headers and response headers
    3. Sets up logger context with correlation ID for the entire request lifecycle
    4. Extracts user_id and workspace_id from headers if available
    """
    
    def __init__(self, app, header_name: str = "X-Correlation-ID"):
        super().__init__(app)
        self.header_name = header_name

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get correlation ID from request headers or generate a new one
        correlation_id = request.headers.get(self.header_name, str(uuid.uuid4()))
        
        # Extract user_id and workspace_id from headers if available
        # Try multiple header name variations
        # user_id = (
        #     request.headers.get("user-id") or 
        #     request.headers.get("user_id") or 
        #     request.headers.get("User-Id") or 
        #     request.headers.get("USER-ID") or
        #     ""
        # )
        # workspace_id = (
        #     request.headers.get("workspace-id") or 
        #     request.headers.get("workspace_id") or 
        #     request.headers.get("Workspace-Id") or 
        #     request.headers.get("WORKSPACE-ID") or
        #     ""
        # )
        
        # Add correlation ID to request state for use in handlers
        request.state.correlation_id = correlation_id
        
        # Configure logger context for this request
        # get_logger_context(
        #     user_id=user_id if user_id else "",  # Pass empty string instead of None
        #     workspace_id=workspace_id if workspace_id else "",  # Pass empty string instead of None
        #     correlation_id=correlation_id
        # )
        
        try:
            # Process the request
            response = await call_next(request)
        finally:
            # Note: Context variables will be automatically cleared when the request context ends
            # No need to manually clear them here as it interferes with async logging
            pass
        
        # Add correlation ID to response headers
        response.headers[self.header_name] = correlation_id
        
        return response