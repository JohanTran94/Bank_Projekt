"""ETL 45837d63
Run ID: 45837d63-5046-4334-9dce-fe3697f96e8d

Revision ID: 24b04d40db08
Revises: 5228d42011b3
Create Date: 2025-06-04 11:25:53.530546

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '24b04d40db08'
down_revision: Union[str, None] = '5228d42011b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 45837d63-5046-4334-9dce-fe3697f96e8d")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '45837d63-5046-4334-9dce-fe3697f96e8d'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '45837d63-5046-4334-9dce-fe3697f96e8d'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '45837d63-5046-4334-9dce-fe3697f96e8d'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '45837d63-5046-4334-9dce-fe3697f96e8d'")
