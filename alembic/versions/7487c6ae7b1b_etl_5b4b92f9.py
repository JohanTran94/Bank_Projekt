"""ETL 5b4b92f9
Run ID: 5b4b92f9-a968-4cec-836e-c426bf20bae6

Revision ID: 7487c6ae7b1b
Revises: 83a36aa64e94
Create Date: 2025-06-04 13:43:47.265529

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7487c6ae7b1b'
down_revision: Union[str, None] = '83a36aa64e94'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 5b4b92f9-a968-4cec-836e-c426bf20bae6")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '5b4b92f9-a968-4cec-836e-c426bf20bae6'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '5b4b92f9-a968-4cec-836e-c426bf20bae6'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '5b4b92f9-a968-4cec-836e-c426bf20bae6'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '5b4b92f9-a968-4cec-836e-c426bf20bae6'")
