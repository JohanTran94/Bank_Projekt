"""ETL 42b34cf0
Run ID: 42b34cf0-6090-4c05-be96-5f0486e78409

Revision ID: 332727b10ce3
Revises: 026b5b7435ca
Create Date: 2025-06-04 11:08:28.292440

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '332727b10ce3'
down_revision: Union[str, None] = '026b5b7435ca'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 42b34cf0-6090-4c05-be96-5f0486e78409")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '42b34cf0-6090-4c05-be96-5f0486e78409'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '42b34cf0-6090-4c05-be96-5f0486e78409'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '42b34cf0-6090-4c05-be96-5f0486e78409'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '42b34cf0-6090-4c05-be96-5f0486e78409'")
