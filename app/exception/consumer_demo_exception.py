from app.exception.baseapp_exception import BaseAppException


class ConsumerDemoException(BaseAppException):
    """Base exception for all Consumer Demo related errors."""


class ConsumerDemoValidationException(ConsumerDemoException):
    """Exception for validation errors in consumer"""
    
    def __init__(self, queue_name: str, validation_error: str):
        super().__init__(
            f"Message validation failed for queue '{queue_name}': {validation_error}"
        )


class ConsumerDemoJobException(ConsumerDemoException):
    """Exception for job execution errors in consumer"""
    
    def __init__(self, queue_name: str, job_name: str, job_error: str):
        super().__init__(
            f"Job '{job_name}' failed for queue '{queue_name}': {job_error}"
        )


class JobDemoServiceException(ConsumerDemoException):
    """Exception for service errors in jobs"""
    
    def __init__(self, job_name: str, service_name: str, service_error: str):
        super().__init__(
            f"Service '{service_name}' failed in job '{job_name}': {service_error}"
        )