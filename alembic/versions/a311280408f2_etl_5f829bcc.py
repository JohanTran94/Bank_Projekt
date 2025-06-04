"""ETL 5f829bcc
Run ID: 5f829bcc-d4a9-4607-9c65-015b174c5f65

Revision ID: a311280408f2
Revises: 1c5169d4a96d
Create Date: 2025-06-04 17:01:17.740082

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a311280408f2'
down_revision: Union[str, None] = '1c5169d4a96d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass


def upgrade():
    print("Data load completed externally. Run ID: 5f829bcc-d4a9-4607-9c65-015b174c5f65")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '5f829bcc-d4a9-4607-9c65-015b174c5f65'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '5f829bcc-d4a9-4607-9c65-015b174c5f65'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '5f829bcc-d4a9-4607-9c65-015b174c5f65'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '5f829bcc-d4a9-4607-9c65-015b174c5f65'")
