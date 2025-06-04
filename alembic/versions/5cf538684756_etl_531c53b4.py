"""ETL 531c53b4
Run ID: 531c53b4-55e9-4691-8b4c-44daacfb2f6f

Revision ID: 5cf538684756
Revises: b5eacdd7c84b
Create Date: 2025-06-04 11:56:06.039117

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5cf538684756'
down_revision: Union[str, None] = 'b5eacdd7c84b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 531c53b4-55e9-4691-8b4c-44daacfb2f6f")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '531c53b4-55e9-4691-8b4c-44daacfb2f6f'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '531c53b4-55e9-4691-8b4c-44daacfb2f6f'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '531c53b4-55e9-4691-8b4c-44daacfb2f6f'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '531c53b4-55e9-4691-8b4c-44daacfb2f6f'")
