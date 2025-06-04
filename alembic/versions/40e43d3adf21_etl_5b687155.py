"""ETL 5b687155
Run ID: 5b687155-13c4-4988-83ce-2ae696bf27c2

Revision ID: 40e43d3adf21
Revises: 62f7228fa01f
Create Date: 2025-06-04 12:27:36.139648

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '40e43d3adf21'
down_revision: Union[str, None] = '62f7228fa01f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 5b687155-13c4-4988-83ce-2ae696bf27c2")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '5b687155-13c4-4988-83ce-2ae696bf27c2'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '5b687155-13c4-4988-83ce-2ae696bf27c2'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '5b687155-13c4-4988-83ce-2ae696bf27c2'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '5b687155-13c4-4988-83ce-2ae696bf27c2'")
