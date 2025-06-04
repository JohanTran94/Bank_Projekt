"""ETL 786e6f33
Run ID: 786e6f33-4c18-4ebd-b515-08dc2d145c9e

Revision ID: 329f98acecc8
Revises: 332727b10ce3
Create Date: 2025-06-04 11:18:34.793950

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '329f98acecc8'
down_revision: Union[str, None] = '332727b10ce3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 786e6f33-4c18-4ebd-b515-08dc2d145c9e")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '786e6f33-4c18-4ebd-b515-08dc2d145c9e'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '786e6f33-4c18-4ebd-b515-08dc2d145c9e'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '786e6f33-4c18-4ebd-b515-08dc2d145c9e'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '786e6f33-4c18-4ebd-b515-08dc2d145c9e'")
