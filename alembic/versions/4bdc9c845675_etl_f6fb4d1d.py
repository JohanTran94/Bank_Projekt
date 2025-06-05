"""ETL f6fb4d1d
Run ID: f6fb4d1d-4d06-4959-9ec6-db0f83e2859d

Revision ID: 4bdc9c845675
Revises: c8c04cd4d984
Create Date: 2025-06-04 11:44:46.787999

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4bdc9c845675'
down_revision: Union[str, None] = 'c8c04cd4d984'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: f6fb4d1d-4d06-4959-9ec6-db0f83e2859d")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'f6fb4d1d-4d06-4959-9ec6-db0f83e2859d'")
