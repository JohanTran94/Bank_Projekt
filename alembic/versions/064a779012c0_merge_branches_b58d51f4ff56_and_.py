"""Merge branches b58d51f4ff56 and e4b556490222

Revision ID: 064a779012c0
Revises: b58d51f4ff56, e4b556490222
Create Date: 2025-06-03 09:56:53.284007

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '064a779012c0'
down_revision: Union[str, None] = ('b58d51f4ff56', 'e4b556490222')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
