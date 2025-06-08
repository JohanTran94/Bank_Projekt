"""ETL 01d9eee8
Run ID: 01d9eee8-cdbe-4daa-a11a-eb523ce510e8

Revision ID: efb3bbd57e84
Revises: 1737d457f5b5
Create Date: 2025-06-08 16:08:15.001417

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'efb3bbd57e84'
down_revision: Union[str, None] = '1737d457f5b5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 01d9eee8-cdbe-4daa-a11a-eb523ce510e8")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '01d9eee8-cdbe-4daa-a11a-eb523ce510e8'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '01d9eee8-cdbe-4daa-a11a-eb523ce510e8'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '01d9eee8-cdbe-4daa-a11a-eb523ce510e8'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '01d9eee8-cdbe-4daa-a11a-eb523ce510e8'")
