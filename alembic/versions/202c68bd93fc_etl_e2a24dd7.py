"""ETL e2a24dd7
Run ID: e2a24dd7-b031-42e4-bf6f-dfeec70e9aa7

Revision ID: 202c68bd93fc
Revises: dcbecbcb31a9
Create Date: 2025-06-04 12:28:52.558935

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '202c68bd93fc'
down_revision: Union[str, None] = 'dcbecbcb31a9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: e2a24dd7-b031-42e4-bf6f-dfeec70e9aa7")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'e2a24dd7-b031-42e4-bf6f-dfeec70e9aa7'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'e2a24dd7-b031-42e4-bf6f-dfeec70e9aa7'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'e2a24dd7-b031-42e4-bf6f-dfeec70e9aa7'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'e2a24dd7-b031-42e4-bf6f-dfeec70e9aa7'")
