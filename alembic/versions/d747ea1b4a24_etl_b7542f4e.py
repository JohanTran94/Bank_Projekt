"""ETL b7542f4e
Run ID: b7542f4e-61ba-43e8-b73e-9b4e513a04dc

Revision ID: d747ea1b4a24
Revises: 7487c6ae7b1b
Create Date: 2025-06-04 14:26:25.896976

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd747ea1b4a24'
down_revision: Union[str, None] = '7487c6ae7b1b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: b7542f4e-61ba-43e8-b73e-9b4e513a04dc")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = 'b7542f4e-61ba-43e8-b73e-9b4e513a04dc'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = 'b7542f4e-61ba-43e8-b73e-9b4e513a04dc'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = 'b7542f4e-61ba-43e8-b73e-9b4e513a04dc'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = 'b7542f4e-61ba-43e8-b73e-9b4e513a04dc'")
