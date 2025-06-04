"""ETL 91545b92
Run ID: 91545b92-3d73-4e8a-9a5c-57938a3294dd

Revision ID: 605f2b75a565
Revises: 026b5b7435ca
Create Date: 2025-06-04 11:26:11.361554

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '605f2b75a565'
down_revision: Union[str, None] = '026b5b7435ca'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 91545b92-3d73-4e8a-9a5c-57938a3294dd")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '91545b92-3d73-4e8a-9a5c-57938a3294dd'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '91545b92-3d73-4e8a-9a5c-57938a3294dd'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '91545b92-3d73-4e8a-9a5c-57938a3294dd'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '91545b92-3d73-4e8a-9a5c-57938a3294dd'")
