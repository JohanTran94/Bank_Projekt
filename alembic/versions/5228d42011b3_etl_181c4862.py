"""ETL 181c4862
Run ID: 181c4862-7e52-4418-9338-4a7c2e93a559

Revision ID: 5228d42011b3
Revises: 329f98acecc8
Create Date: 2025-06-04 11:23:21.108170

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5228d42011b3'
down_revision: Union[str, None] = '329f98acecc8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 181c4862-7e52-4418-9338-4a7c2e93a559")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '181c4862-7e52-4418-9338-4a7c2e93a559'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '181c4862-7e52-4418-9338-4a7c2e93a559'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '181c4862-7e52-4418-9338-4a7c2e93a559'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '181c4862-7e52-4418-9338-4a7c2e93a559'")
