"""ETL c6d5e17e
Run ID: c6d5e17e-c443-48ee-9091-17dff71e41ae

Revision ID: b5eacdd7c84b
Revises: 3c8c5c836eaf
Create Date: 2025-06-04 11:41:44.621661

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b5eacdd7c84b'
down_revision: Union[str, None] = '3c8c5c836eaf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: c6d5e17e-c443-48ee-9091-17dff71e41ae")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'c6d5e17e-c443-48ee-9091-17dff71e41ae'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'c6d5e17e-c443-48ee-9091-17dff71e41ae'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'c6d5e17e-c443-48ee-9091-17dff71e41ae'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'c6d5e17e-c443-48ee-9091-17dff71e41ae'")
