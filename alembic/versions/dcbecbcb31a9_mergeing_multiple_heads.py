"""mergeing multiple heads

Revision ID: dcbecbcb31a9
Revises: 5cf538684756, 9bb6cbd865dd
Create Date: 2025-06-04 12:28:09.421535

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dcbecbcb31a9'
down_revision: Union[str, None] = ('5cf538684756', '9bb6cbd865dd')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
