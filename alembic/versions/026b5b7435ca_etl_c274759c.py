"""ETL c274759c
Run ID: c274759c-a64a-4c38-b826-074d77d9c9aa

Revision ID: 026b5b7435ca
Revises: eaa0bc9e123f
Create Date: 2025-06-04 10:13:37.092687

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '026b5b7435ca'
down_revision: Union[str, None] = 'eaa0bc9e123f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: c274759c-a64a-4c38-b826-074d77d9c9aa")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'c274759c-a64a-4c38-b826-074d77d9c9aa'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'c274759c-a64a-4c38-b826-074d77d9c9aa'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'c274759c-a64a-4c38-b826-074d77d9c9aa'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'c274759c-a64a-4c38-b826-074d77d9c9aa'")
