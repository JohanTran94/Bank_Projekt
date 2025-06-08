"""ETL d03d7b6b
Run ID: d03d7b6b-8c45-49ba-b566-54c8ef2db64b

Revision ID: 7e0e199f27e2
Revises: 889476be1c33
Create Date: 2025-06-05 06:50:22.483779

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7e0e199f27e2'
down_revision: Union[str, None] = '889476be1c33'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: d03d7b6b-8c45-49ba-b566-54c8ef2db64b")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'd03d7b6b-8c45-49ba-b566-54c8ef2db64b'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'd03d7b6b-8c45-49ba-b566-54c8ef2db64b'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'd03d7b6b-8c45-49ba-b566-54c8ef2db64b'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'd03d7b6b-8c45-49ba-b566-54c8ef2db64b'")
