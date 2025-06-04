"""ETL fced96a2
Run ID: fced96a2-306d-46dd-91b9-b75f5fd217d2

Revision ID: 1c5169d4a96d
Revises: d747ea1b4a24
Create Date: 2025-06-04 16:07:52.213230

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1c5169d4a96d'
down_revision: Union[str, None] = 'd747ea1b4a24'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: fced96a2-306d-46dd-91b9-b75f5fd217d2")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'fced96a2-306d-46dd-91b9-b75f5fd217d2'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'fced96a2-306d-46dd-91b9-b75f5fd217d2'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'fced96a2-306d-46dd-91b9-b75f5fd217d2'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'fced96a2-306d-46dd-91b9-b75f5fd217d2'")
