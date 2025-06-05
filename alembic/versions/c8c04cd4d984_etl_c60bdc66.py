"""ETL c60bdc66
Run ID: c60bdc66-225f-4b84-b157-c41a3d649225

Revision ID: c8c04cd4d984
Revises: 026b5b7435ca
Create Date: 2025-06-04 11:39:14.017105

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c8c04cd4d984'
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
    print("Data load completed externally. Run ID: c60bdc66-225f-4b84-b157-c41a3d649225")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'c60bdc66-225f-4b84-b157-c41a3d649225'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'c60bdc66-225f-4b84-b157-c41a3d649225'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'c60bdc66-225f-4b84-b157-c41a3d649225'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'c60bdc66-225f-4b84-b157-c41a3d649225'")
