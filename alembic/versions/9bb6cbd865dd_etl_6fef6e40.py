"""ETL 6fef6e40
Run ID: 6fef6e40-2bbb-4d66-9f77-d7da4e600b8b

Revision ID: 9bb6cbd865dd
Revises: f0b6ae029463
Create Date: 2025-06-04 11:53:29.630181

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9bb6cbd865dd'
down_revision: Union[str, None] = 'f0b6ae029463'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 6fef6e40-2bbb-4d66-9f77-d7da4e600b8b")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '6fef6e40-2bbb-4d66-9f77-d7da4e600b8b'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '6fef6e40-2bbb-4d66-9f77-d7da4e600b8b'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '6fef6e40-2bbb-4d66-9f77-d7da4e600b8b'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '6fef6e40-2bbb-4d66-9f77-d7da4e600b8b'")
