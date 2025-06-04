"""ETL 199da7f7
Run ID: 199da7f7-8808-4f7e-873f-bf4a43312eb2

Revision ID: 62f7228fa01f
Revises: 5cf538684756
Create Date: 2025-06-04 12:20:20.288545

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '62f7228fa01f'
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
    print("Data load completed externally. Run ID: 199da7f7-8808-4f7e-873f-bf4a43312eb2")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '199da7f7-8808-4f7e-873f-bf4a43312eb2'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '199da7f7-8808-4f7e-873f-bf4a43312eb2'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '199da7f7-8808-4f7e-873f-bf4a43312eb2'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '199da7f7-8808-4f7e-873f-bf4a43312eb2'")
