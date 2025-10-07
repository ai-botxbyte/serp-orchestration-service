"""Initial migration

Revision ID: 3358703046c8
Revises: 
Create Date: 2025-09-24 17:06:37.335037

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3358703046c8'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create user_activities table
    op.create_table('user_activities',
        sa.Column('user_activity_log_id', sa.UUID(), nullable=False),
        sa.Column('service_name', sa.String(length=255), nullable=False),
        sa.Column('correlation_id', sa.String(length=36), nullable=True),
        sa.Column('workspace_id', sa.UUID(), nullable=False),
        sa.Column('user_id', sa.UUID(), nullable=True),
        sa.Column('action_type', sa.String(length=100), nullable=True),
        sa.Column('message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('deleted_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.UUID(), nullable=False),
        sa.Column('updated_by', sa.UUID(), nullable=True),
        sa.Column('deleted_by', sa.UUID(), nullable=True),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('error_user_message', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('user_activity_log_id')
    )
    
    # Create central_logs table
    op.create_table('central_logs',
        sa.Column('central_log_id', sa.UUID(), nullable=False),
        sa.Column('service_name', sa.String(length=255), nullable=False),
        sa.Column('correlation_id', sa.String(length=36), nullable=True),
        sa.Column('level', sa.String(length=20), nullable=False),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('deleted_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.UUID(), nullable=False),
        sa.Column('updated_by', sa.UUID(), nullable=True),
        sa.Column('deleted_by', sa.UUID(), nullable=True),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('error_user_message', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('central_log_id')
    )


def downgrade() -> None:
    op.drop_table('central_logs')
    op.drop_table('user_activities')
