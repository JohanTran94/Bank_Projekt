"""merge heads

Revision ID: eaa0bc9e123f
Revises: 908966ddd564, a9276aab9341
Create Date: 2025-06-03 14:33:16.217980

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eaa0bc9e123f'
down_revision: Union[str, None] = ('908966ddd564', 'a9276aab9341')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
