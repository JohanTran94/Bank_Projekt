"""ETL 2fdffbbe
Run ID: 2fdffbbe-7038-47f0-9f92-9adb651b20d6

Revision ID: 751f9c285783
Revises: 5cf538684756
Create Date: 2025-06-06 10:27:06.412574

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '751f9c285783'
down_revision: Union[str, None] = '5cf538684756'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 2fdffbbe-7038-47f0-9f92-9adb651b20d6")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '2fdffbbe-7038-47f0-9f92-9adb651b20d6'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '2fdffbbe-7038-47f0-9f92-9adb651b20d6'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '2fdffbbe-7038-47f0-9f92-9adb651b20d6'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '2fdffbbe-7038-47f0-9f92-9adb651b20d6'")
