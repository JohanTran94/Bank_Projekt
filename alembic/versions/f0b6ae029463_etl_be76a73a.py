"""ETL be76a73a
Run ID: be76a73a-2890-4378-9c3b-220e309e9e2c

Revision ID: f0b6ae029463
Revises: 4115739fb23c
Create Date: 2025-06-04 11:49:26.414966

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f0b6ae029463'
down_revision: Union[str, None] = '4115739fb23c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: be76a73a-2890-4378-9c3b-220e309e9e2c")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'be76a73a-2890-4378-9c3b-220e309e9e2c'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'be76a73a-2890-4378-9c3b-220e309e9e2c'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'be76a73a-2890-4378-9c3b-220e309e9e2c'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'be76a73a-2890-4378-9c3b-220e309e9e2c'")
