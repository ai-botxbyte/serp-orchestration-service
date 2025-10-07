from __future__ import annotations

from typing import Any, Generic, TypeVar, Optional, List, Union
from pydantic import BaseModel, Field, ConfigDict

T = TypeVar("T")

class ErrorDetail(BaseModel):
    """Individual error detail."""
    type: str = Field(..., description="Error type")
    loc: List[str] = Field(..., description="Error location")
    msg: str = Field(..., description="Error message")
    input: Any = Field(None, description="Input that caused the error")


class StandardResponse(BaseModel, Generic[T]):
    """Standard API response format."""
    success: bool = Field(..., description="Whether the operation was successful")
    data: Optional[T] = Field(None, description="Response data")
    error_message: Optional[str] = Field(None, description="Error message if any")
    errors: List[ErrorDetail] = Field(default_factory=list, description="Detailed error information")


class SuccessResponse(StandardResponse[T]):
    """Success response format."""
    success: bool = Field(True, description="Operation was successful")
    error_message: Optional[str] = Field(None, description="No error message for success")
    errors: List[ErrorDetail] = Field(default_factory=list, description="No errors for success")


class ErrorResponse(StandardResponse[None]):
    """Error response format."""
    success: bool = Field(False, description="Operation failed")
    data: Optional[None] = Field(None, description="No data for errors")


class ApiResponseSchema(BaseModel, Generic[T]):
    """Standard API response wrapper for consistency."""

    success: bool = True
    data: Optional[Union[T, list[T], dict]] = Field(default_factory=dict)
    message: str = "Operation completed successfully"

    model_config = ConfigDict(extra="forbid")


class PaginationMeta(BaseModel):
    """Pagination metadata for list responses."""
    
    total_count: int = Field(..., description="Total number of records")
    offset: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(default=100, ge=1, le=1000, description="Number of items to return")
    total_pages: int = Field(..., description="Total number of pages")
    
    model_config = ConfigDict(from_attributes=True)


class PaginatedResponseSchema(BaseModel, Generic[T]):
    """Paginated API response wrapper."""
    
    success: bool = True
    data: Optional[Union[T, list[T], dict]] = Field(default_factory=dict)
    pagination: Optional[PaginationMeta] = None
    message: str = "Operation completed successfully"
    
    model_config = ConfigDict(extra="forbid")



class ListParamsSchema(BaseModel):
    """Schema for list API parameters."""
    offset: int = Field(default=0, ge=0, description="Number of records to skip")
    limit: int = Field(default=100, ge=1, le=1000, description="Number of items to return")
    order_by: str = Field(default="-created_at", description="Field to order by. Prefix with '-' for descending order")
    search: Optional[str] = Field(default=None, description="Search query string to filter results")
    filters: Optional[List[str]] = Field(default=None, description="List of filter dicts as JSON strings")