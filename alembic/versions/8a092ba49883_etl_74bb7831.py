"""ETL 74bb7831
Run ID: 74bb7831-65cf-4029-a02d-4002277b61fb

Revision ID: 8a092ba49883
Revises: a311280408f2
Create Date: 2025-06-04 17:12:30.488876

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8a092ba49883'
down_revision: Union[str, None] = 'a311280408f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 74bb7831-65cf-4029-a02d-4002277b61fb")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '74bb7831-65cf-4029-a02d-4002277b61fb'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '74bb7831-65cf-4029-a02d-4002277b61fb'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '74bb7831-65cf-4029-a02d-4002277b61fb'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '74bb7831-65cf-4029-a02d-4002277b61fb'")
