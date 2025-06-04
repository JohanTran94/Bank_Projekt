"""ETL 11cc81e6
Run ID: 11cc81e6-99d0-4713-8b8a-b6b896244dca

Revision ID: 83a36aa64e94
Revises: 40e43d3adf21
Create Date: 2025-06-04 12:55:20.470945

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '83a36aa64e94'
down_revision: Union[str, None] = '40e43d3adf21'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 11cc81e6-99d0-4713-8b8a-b6b896244dca")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '11cc81e6-99d0-4713-8b8a-b6b896244dca'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '11cc81e6-99d0-4713-8b8a-b6b896244dca'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '11cc81e6-99d0-4713-8b8a-b6b896244dca'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '11cc81e6-99d0-4713-8b8a-b6b896244dca'")
