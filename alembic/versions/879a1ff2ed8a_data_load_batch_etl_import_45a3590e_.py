"""Data load: Batch ETL import [45a3590e-f820-4705-ad31-18c1a4c60ace]"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "879a1ff2ed8a_data_load_batch_etl_import_45a3590e_"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    print("Data load completed externally. Run ID: 45a3590e-f820-4705-ad31-18c1a4c60ace")


def downgrade():
    conn = op.get_bind()
    for table in ["transactions", "accounts", "customers", "transaction_locations"]:
        conn.execute(f"DELETE FROM {table} WHERE migration_run_id = '45a3590e-f820-4705-ad31-18c1a4c60ace'")
