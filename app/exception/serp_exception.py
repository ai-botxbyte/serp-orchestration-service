"""SERP Orchestration Service Exceptions."""

from app.exception.baseapp_exception import BaseAppException


class SerpOrchestrationException(BaseAppException):
    """Base exception for all SERP orchestration related errors."""


class SerpServiceCallException(SerpOrchestrationException):
    """Exception for SERP lambda service call errors."""

    def __init__(self, service_url: str, error: str):
        self.service_url = service_url
        self.error = error
        message = f"SERP service call to '{service_url}' failed: {error}"
        super().__init__(message=message)


class SerpRequestValidationException(SerpOrchestrationException):
    """Exception for SERP request validation errors."""

    def __init__(self, validation_error: str):
        self.validation_error = validation_error
        message = f"SERP request validation failed: {validation_error}"
        super().__init__(message=message)


class SerpResponseException(SerpOrchestrationException):
    """Exception for SERP response processing errors."""

    def __init__(self, error: str):
        self.error = error
        message = f"SERP response processing failed: {error}"
        super().__init__(message=message)


class SerpJobException(SerpOrchestrationException):
    """Exception for SERP job execution errors."""

    def __init__(self, job_name: str, error: str):
        self.job_name = job_name
        self.error = error
        message = f"SERP job '{job_name}' failed: {error}"
        super().__init__(message=message)


class SerpQueueException(SerpOrchestrationException):
    """Exception for SERP queue operation errors."""

    def __init__(self, queue_name: str, operation: str, error: str):
        self.queue_name = queue_name
        self.operation = operation
        self.error = error
        message = f"SERP queue '{queue_name}' {operation} failed: {error}"
        super().__init__(message=message)


class SerpDLXException(SerpOrchestrationException):
    """Exception for SERP DLX (Dead Letter Exchange) errors."""

    def __init__(self, original_queue: str, dlx_queue: str, error: str):
        self.original_queue = original_queue
        self.dlx_queue = dlx_queue
        self.error = error
        message = f"SERP DLX operation from '{original_queue}' to '{dlx_queue}' failed: {error}"
        super().__init__(message=message)


class SerpRetryException(SerpOrchestrationException):
    """Exception for SERP retry operation errors."""

    def __init__(self, attempt: int, max_attempts: int, error: str):
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.error = error
        message = f"SERP retry attempt {attempt}/{max_attempts} failed: {error}"
        super().__init__(message=message)
