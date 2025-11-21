"""RabbitMQ schemas for queue and message management."""

from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


class MessagePublishSchema(BaseModel):
    """Schema for publishing a message to a single queue."""

    queue_name: str = Field(
        ..., description="Name of the queue to publish to", example="orders_queue"
    )
    message: dict[str, Any] | str = Field(
        ...,
        description="Message content (dict or string)",
        example="Order #12345 has been created",
    )


class ExchangeSetupSchema(BaseModel):
    """Schema for setting up an exchange with queues (one-time setup)."""

    exchange_name: str = Field(
        ..., description="Name of the exchange to create", example="notifications"
    )
    queue_names: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "List of queue names to bind to exchange. "
            "Queues will be created if they don't exist."
        ),
        example=["email_queue", "sms_queue", "push_queue"],
    )


class ExchangePublishSchema(BaseModel):
    """Schema for publishing a message to an exchange (broadcasts to all bound queues)."""

    exchange_name: str = Field(
        ..., description="Name of the exchange to publish to", example="notifications"
    )
    message: dict[str, Any] | str = Field(
        ...,
        description="Message to broadcast to all queues bound to this exchange",
        example="User registered successfully",
    )


class ConsumerDetailsSchema(BaseModel):
    """Schema for consumer details."""

    consumer_tag: Optional[str] = Field(None, description="Consumer tag identifier")
    channel_details: Optional[dict[str, Any]] = Field(
        None, description="Details about the consumer's channel"
    )
    queue: Optional[dict[str, Any]] = Field(None, description="Queue information")
    prefetch_count: Optional[int] = Field(None, description="Consumer prefetch count")
    ack_required: Optional[bool] = Field(
        None, description="Whether acknowledgements are required"
    )


class MessageStatsSchema(BaseModel):
    """Schema for message statistics."""

    publish: Optional[int] = Field(
        None, description="Total number of messages published to the queue"
    )
    publish_details: Optional[dict[str, Any]] = Field(
        None, description="Message publish rate details"
    )
    deliver_get: Optional[int] = Field(
        None, description="Total number of messages delivered to consumers"
    )
    deliver_get_details: Optional[dict[str, Any]] = Field(
        None, description="Message delivery rate details"
    )
    ack: Optional[int] = Field(
        None, description="Total number of acknowledged messages"
    )
    ack_details: Optional[dict[str, Any]] = Field(
        None, description="Message acknowledgement rate details"
    )
    deliver: Optional[int] = Field(
        None, description="Total number of messages delivered"
    )
    deliver_details: Optional[dict[str, Any]] = Field(
        None, description="Message delivery rate details"
    )
    redeliver: Optional[int] = Field(
        None, description="Total number of redelivered messages"
    )
    redeliver_details: Optional[dict[str, Any]] = Field(
        None, description="Message redelivery rate details"
    )


class QueueInfoResponseSchema(BaseModel):
    """Schema for detailed queue information response."""

    name: Optional[str] = Field(None, description="Queue name")
    vhost: Optional[str] = Field(None, description="Virtual host")
    durable: Optional[bool] = Field(None, description="Whether queue is durable")
    auto_delete: Optional[bool] = Field(None, description="Whether queue auto-deletes")
    consumers: list[dict[str, Any]] = Field(
        default_factory=list, description="List of active consumers"
    )
    consumer_count: int = Field(
        default=0, description="Number of active consumers consuming from the queue"
    )
    messages: int = Field(default=0, description="Total messages in queue")
    messages_ready: int = Field(
        default=0,
        description="Number of messages ready to be delivered (pending messages)",
    )
    messages_unacknowledged: int = Field(
        default=0,
        description="Number of messages delivered but not yet acknowledged",
    )
    message_stats: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Message statistics including counts and rates for publish, deliver, "
            "ack, and other operations"
        ),
    )
    state: Optional[str] = Field(None, description="Queue state (running, idle, etc.)")
    node: Optional[str] = Field(None, description="RabbitMQ node hosting this queue")
