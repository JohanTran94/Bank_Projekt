"""ETL a3c53b93
Run ID: a3c53b93-d62d-4d22-af75-125084944b59

Revision ID: 889476be1c33
Revises: d77015afeefa
Create Date: 2025-06-04 21:02:48.428267

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '889476be1c33'
down_revision: Union[str, None] = 'd77015afeefa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: a3c53b93-d62d-4d22-af75-125084944b59")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'a3c53b93-d62d-4d22-af75-125084944b59'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'a3c53b93-d62d-4d22-af75-125084944b59'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'a3c53b93-d62d-4d22-af75-125084944b59'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'a3c53b93-d62d-4d22-af75-125084944b59'")
