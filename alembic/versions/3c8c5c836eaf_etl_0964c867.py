"""ETL 0964c867
Run ID: 0964c867-569b-49d9-9473-b5effccc0405

Revision ID: 3c8c5c836eaf
Revises: 605f2b75a565
Create Date: 2025-06-04 11:32:00.298532

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3c8c5c836eaf'
down_revision: Union[str, None] = '605f2b75a565'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 0964c867-569b-49d9-9473-b5effccc0405")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '0964c867-569b-49d9-9473-b5effccc0405'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '0964c867-569b-49d9-9473-b5effccc0405'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '0964c867-569b-49d9-9473-b5effccc0405'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '0964c867-569b-49d9-9473-b5effccc0405'")
