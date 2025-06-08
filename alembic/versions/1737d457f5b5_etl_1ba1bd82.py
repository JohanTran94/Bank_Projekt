"""ETL 1ba1bd82
Run ID: 1ba1bd82-9d69-4886-8fe3-415d3bbbd734

Revision ID: 1737d457f5b5
Revises: 7e0e199f27e2
Create Date: 2025-06-05 09:27:21.188699

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1737d457f5b5'
down_revision: Union[str, None] = '7e0e199f27e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 1ba1bd82-9d69-4886-8fe3-415d3bbbd734")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '1ba1bd82-9d69-4886-8fe3-415d3bbbd734'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '1ba1bd82-9d69-4886-8fe3-415d3bbbd734'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '1ba1bd82-9d69-4886-8fe3-415d3bbbd734'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '1ba1bd82-9d69-4886-8fe3-415d3bbbd734'")
