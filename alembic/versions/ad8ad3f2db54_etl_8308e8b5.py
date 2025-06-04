"""ETL 8308e8b5
Run ID: 8308e8b5-17ac-4a2d-ac91-b243a68fe72e

Revision ID: ad8ad3f2db54
Revises: 8a092ba49883
Create Date: 2025-06-04 17:45:03.184124

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ad8ad3f2db54'
down_revision: Union[str, None] = '8a092ba49883'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 8308e8b5-17ac-4a2d-ac91-b243a68fe72e")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '8308e8b5-17ac-4a2d-ac91-b243a68fe72e'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '8308e8b5-17ac-4a2d-ac91-b243a68fe72e'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '8308e8b5-17ac-4a2d-ac91-b243a68fe72e'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '8308e8b5-17ac-4a2d-ac91-b243a68fe72e'")
