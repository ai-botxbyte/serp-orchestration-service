from __future__ import annotations

from typing import Any, Generic, TypeVar, Optional, List, Union, Dict
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
    errors: List[ErrorDetail] = Field(
        default_factory=list, description="Detailed error information"
    )


class SuccessResponse(StandardResponse[T]):
    """Success response format."""

    success: bool = Field(True, description="Operation was successful")
    error_message: Optional[str] = Field(
        None, description="No error message for success"
    )
    errors: List[ErrorDetail] = Field(
        default_factory=list, description="No errors for success"
    )


class ApiResponseSchema(BaseModel, Generic[T]):
    """Standard API response wrapper for consistency."""

    success: bool = True
    data: Optional[Union[T, list[T], dict]] = Field(default_factory=dict)
    message: str = "Operation completed successfully"

    model_config = ConfigDict(extra="forbid")


class PaginationMeta(BaseModel):
    """Pagination metadata for list responses."""

    total_count: int
    offset: int
    limit: int
    total_pages: int

    model_config = ConfigDict(from_attributes=True)


class PaginatedResponseSchema(BaseModel, Generic[T]):
    """Paginated API response wrapper."""

    success: bool = True
    data: Optional[Union[T, list[T], dict]] = Field(default_factory=dict)
    pagination: Optional[PaginationMeta] = None
    message: str = "Operation completed successfully"
    model_config = ConfigDict(extra="forbid")


class ListParamsSchema(BaseModel):
    """Schema for list parameters."""

    offset: int
    limit: int
    order_by: str
    search: Optional[str]
    filters: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]
