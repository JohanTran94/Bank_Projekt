"""ETL 9636671a
Run ID: 9636671a-3c23-43a9-b089-b13acab395c4

Revision ID: d77015afeefa
Revises: ad8ad3f2db54
Create Date: 2025-06-04 20:56:29.512280

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd77015afeefa'
down_revision: Union[str, None] = 'ad8ad3f2db54'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 9636671a-3c23-43a9-b089-b13acab395c4")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '9636671a-3c23-43a9-b089-b13acab395c4'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '9636671a-3c23-43a9-b089-b13acab395c4'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '9636671a-3c23-43a9-b089-b13acab395c4'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '9636671a-3c23-43a9-b089-b13acab395c4'")
