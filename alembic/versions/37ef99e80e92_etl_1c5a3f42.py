"""ETL 1c5a3f42
Run ID: 1c5a3f42-9e2d-4b13-ac76-11a7c1b5d99d

Revision ID: 37ef99e80e92
Revises: 24b04d40db08
Create Date: 2025-06-04 11:30:57.522977

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '37ef99e80e92'
down_revision: Union[str, None] = '24b04d40db08'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 1c5a3f42-9e2d-4b13-ac76-11a7c1b5d99d")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '1c5a3f42-9e2d-4b13-ac76-11a7c1b5d99d'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '1c5a3f42-9e2d-4b13-ac76-11a7c1b5d99d'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '1c5a3f42-9e2d-4b13-ac76-11a7c1b5d99d'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '1c5a3f42-9e2d-4b13-ac76-11a7c1b5d99d'")
