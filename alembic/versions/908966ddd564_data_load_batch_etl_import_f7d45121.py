"""Data load: Batch ETL import [f7d45121]
Run ID: f7d45121-d7d3-48d7-b11c-16ce4971a0bf

Revision ID: 908966ddd564
Revises: 161e0c8ac199
Create Date: 2025-06-03 11:47:16.317888

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '908966ddd564'
down_revision: Union[str, None] = '161e0c8ac199'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: f7d45121-d7d3-48d7-b11c-16ce4971a0bf")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'f7d45121-d7d3-48d7-b11c-16ce4971a0bf'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'f7d45121-d7d3-48d7-b11c-16ce4971a0bf'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'f7d45121-d7d3-48d7-b11c-16ce4971a0bf'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'f7d45121-d7d3-48d7-b11c-16ce4971a0bf'")
