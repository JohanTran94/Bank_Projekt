"""Merge multiple heads

Revision ID: 161e0c8ac199
Revises: dedb49c7098c, 879a1ff2ed8a_data_load_batch_etl_import_45a3590e_
Create Date: 2025-06-03 11:11:24.220460

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '161e0c8ac199'
down_revision: Union[str, None] = ('dedb49c7098c', '879a1ff2ed8a_data_load_batch_etl_import_45a3590e_')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
