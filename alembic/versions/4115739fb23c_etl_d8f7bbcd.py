"""ETL d8f7bbcd
Run ID: d8f7bbcd-cc86-47fa-b118-e666babfe62d

Revision ID: 4115739fb23c
Revises: 37ef99e80e92
Create Date: 2025-06-04 11:42:06.557847

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4115739fb23c'
down_revision: Union[str, None] = '37ef99e80e92'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: d8f7bbcd-cc86-47fa-b118-e666babfe62d")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'd8f7bbcd-cc86-47fa-b118-e666babfe62d'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'd8f7bbcd-cc86-47fa-b118-e666babfe62d'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'd8f7bbcd-cc86-47fa-b118-e666babfe62d'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'd8f7bbcd-cc86-47fa-b118-e666babfe62d'")
