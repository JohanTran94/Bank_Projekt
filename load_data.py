import os
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
from models import ErrorRow, MigrationRun
from dotenv import load_dotenv
from pathlib import Path
import uuid
from alembic.config import Config
from alembic import command
from collections import defaultdict

MIGRATION_RUN_ID = uuid.uuid4()
error_counts = defaultdict(int)

def tag_dataframe(df, run_id):
    df["migration_run_id"] = str(run_id)
    return df


def create_alembic_revision_if_data_exists(alembic_cfg, description: str, run_id: uuid.UUID, session):
    from sqlalchemy import text

    # Kontrollera om en revision redan finns med detta run_id i alembic_versions
    version_dir = Path("alembic/versions")
    existing_files = list(version_dir.glob("*.py"))
    short_run_id = str(run_id)[:8]
    existing_with_run_id = [f for f in existing_files if short_run_id in f.read_text()]

    if existing_with_run_id:
        print(f"⚠️ Alembic-revision för run_id {short_run_id} finns redan. Hoppar över.")
        return

    # Kontrollera om det finns data med detta run_id
    tables = ["customers", "accounts", "transactions", "transaction_locations"]
    data_found = False

    for table in tables:
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {table} WHERE migration_run_id = :run_id"),
            {"run_id": str(run_id)}
        ).scalar()

        if count > 0:
            data_found = True
            break

    if not data_found:
        print("⚠️ Ingen data med detta run_id hittades – hoppar över Alembic-revision.")
        return

    # Skapa revision
    message = f"Data load: {description} [{short_run_id}]"
    command.revision(alembic_cfg, message=message, autogenerate=False)

    revision_path = sorted(version_dir.glob("*.py"), key=os.path.getmtime)[-1]

    run_id_str = str(run_id)
    content = f"""

def upgrade():
    print("Data load completed externally. Run ID: {run_id_str}")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '{run_id_str}'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '{run_id_str}'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '{run_id_str}'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '{run_id_str}'")
"""
    with open(revision_path, "a", encoding="utf-8") as f:
        f.write(content)

    command.upgrade(alembic_cfg, "head")
    print("✅ Alembic-revision skapad och uppgraderad till head.")



os.chdir(Path(__file__).resolve().parent)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def serialize_for_json(obj):
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(i) for i in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def log_error_row(context: str, reason: str, row: dict, csv_file: str):
    clean_row = serialize_for_json(row)
    error = ErrorRow(
        context=context,
        error_reason=reason,
        raw_data=clean_row,
        logged_at=datetime.utcnow()
    )
    session.add(error)
    session.commit()

    error_counts[reason] += 1

    os.makedirs("_logs", exist_ok=True)
    file_path = os.path.join("_logs", csv_file)

    df = pd.DataFrame([{
        "context": context,
        "error_reason": reason,
        "raw_data": json.dumps(clean_row),
        "logged_at": datetime.utcnow().isoformat()
    }])

    if os.path.exists(file_path):
        df.to_csv(file_path, mode="a", header=False, index=False)
    else:
        df.to_csv(file_path, index=False)

def batch_insert(df: pd.DataFrame, table: str, engine, chunk_size=500, session=None):
    if df.empty:
        return

    df = df.where(pd.notna(df), None)
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(table, con=engine, if_exists="append", index=False, method="multi")
        except IntegrityError as e:
            print(f"\nFel vid batch_insert i tabell '{table}': {e}\n")
            if session:
                session.rollback()

def load_customers_accounts(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    df = df.rename(columns={
        "Customer": "name",
        "Address": "address",
        "Phone": "phone",
        "Personnummer": "ssn",
        "BankAccount": "account_number"
    })

    required_fields = ["name", "ssn", "account_number"]
    invalid_rows = df[df[required_fields].isnull().any(axis=1)]
    valid_rows = df.dropna(subset=required_fields)

    for _, row in invalid_rows.compute().iterrows():
        log_error_row("load_customers_accounts", "Missing required fields", row.to_dict(), "invalid_customers.csv")

    pdf = valid_rows.compute()

    unique_customers = pdf.drop_duplicates(subset="ssn").copy()
    customers_df = unique_customers[["name", "address", "phone", "ssn"]].copy()
    customers_df = tag_dataframe(customers_df, MIGRATION_RUN_ID)
    batch_insert(customers_df, "customers", engine, session=session)

    with engine.connect() as conn:
        customer_map = pd.read_sql("SELECT id, ssn FROM customers", conn)

    merged = pdf.merge(customer_map, on="ssn", how="left")
    accounts_df = merged[["account_number", "id"]].drop_duplicates("account_number")
    accounts_df = accounts_df.rename(columns={"id": "customer_id"})

    accounts_df = tag_dataframe(accounts_df, MIGRATION_RUN_ID)
    batch_insert(accounts_df, "accounts", engine, session=session)

def ensure_transaction_locations_exist(pdf: pd.DataFrame, session=None) -> pd.DataFrame:
    location_cols = ["sender_country", "sender_municipality", "receiver_country", "receiver_municipality"]
    unique_locs = pdf[location_cols].drop_duplicates().copy()
    unique_locs = unique_locs.where(pd.notna(unique_locs), None)

    with engine.connect() as conn:
        existing = pd.read_sql("SELECT * FROM transaction_locations", conn)

    frames = [df.dropna(how="all") for df in [existing, unique_locs]]
    combined = pd.concat(frames).drop_duplicates(location_cols, keep="first").reset_index(drop=True)
    combined["id"] = combined.index + 1

    new_locs = combined[~combined["id"].isin(existing["id"])]
    if not new_locs.empty:
        new_locs = tag_dataframe(new_locs, MIGRATION_RUN_ID)
        batch_insert(new_locs, "transaction_locations", engine, session=session)

    merged = pdf.merge(combined, on=location_cols, how="left")
    return merged

def load_transactions(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    required_fields = ["transaction_id", "sender_account", "receiver_account"]
    missing_fields = df[df[required_fields].isnull().any(axis=1)]
    df = df.dropna(subset=required_fields)

    for _, row in missing_fields.compute().iterrows():
        log_error_row("load_transactions", "Missing required transaction fields", row.to_dict(), "invalid_transactions.csv")

    df["amount"] = df["amount"].map_partitions(lambda s: pd.to_numeric(s, errors="coerce"))
    invalid_amounts = df[df["amount"].map_partitions(lambda x: pd.to_numeric(x, errors="coerce").isna())]
    df = df[df["amount"].map_partitions(lambda x: pd.to_numeric(x, errors="coerce").notna())]

    for _, row in invalid_amounts.compute().iterrows():
        log_error_row("load_transactions", "Invalid amount (not a number)", row.to_dict(), "invalid_amounts.csv")

    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    invalid_dates = df[df["timestamp"].isna()]
    df = df.dropna(subset=["timestamp"])

    for _, row in invalid_dates.compute().iterrows():
        log_error_row("load_transactions", "Invalid timestamp (could not parse date)", row.to_dict(), "invalid_timestamps.csv")

    valid_currencies = {"SEK", "USD", "EUR", "GBP", "CNY"}
    invalid_currency = df[~df["currency"].isin(valid_currencies)]
    df = df[df["currency"].isin(valid_currencies)]

    for _, row in invalid_currency.compute().iterrows():
        log_error_row("load_transactions", "Invalid currency", row.to_dict(), "invalid_currency.csv")

    pdf = df.compute()
    pdf = pdf[pdf["amount"] >= 0]

    pdf = ensure_transaction_locations_exist(pdf)
    pdf["is_internal_sender"] = pdf["sender_account"].str.startswith("SE")

    with engine.connect() as conn:
        existing_accounts = pd.read_sql("SELECT account_number FROM accounts", conn)
    known_accounts = set(existing_accounts["account_number"])

    unknown_senders = pdf[~pdf["sender_account"].isin(known_accounts)]
    internal_missing = unknown_senders[unknown_senders["is_internal_sender"]]
    for _, row in internal_missing.iterrows():
        log_error_row("load_transactions", "Internal sender account missing in accounts table", row.to_dict(), "internal_missing_accounts.csv")

    external_missing = unknown_senders[~unknown_senders["is_internal_sender"]]
    if not external_missing.empty:
        new_external_accounts = pd.DataFrame({
            "account_number": external_missing["sender_account"].unique(),
            "customer_id": None
        })
        new_external_accounts = tag_dataframe(new_external_accounts, MIGRATION_RUN_ID)
        batch_insert(new_external_accounts, "accounts", engine)
        known_accounts.update(new_external_accounts["account_number"])

    pdf = pdf[pdf["sender_account"].isin(known_accounts)].copy()

    unknown_receivers = pdf[~pdf["receiver_account"].isin(known_accounts)]
    internal_receivers = unknown_receivers[unknown_receivers["receiver_account"].str.startswith("SE")]
    for _, row in internal_receivers.iterrows():
        log_error_row("load_transactions", "Internal receiver account missing in accounts table", row.to_dict(), "internal_missing_receivers.csv")

    external_receivers = unknown_receivers[~unknown_receivers["receiver_account"].str.startswith("SE")]
    if not external_receivers.empty:
        new_external_receiver_accounts = pd.DataFrame({
            "account_number": external_receivers["receiver_account"].unique(),
            "customer_id": None
        })
        new_external_receiver_accounts = tag_dataframe(new_external_receiver_accounts, MIGRATION_RUN_ID)
        batch_insert(new_external_receiver_accounts, "accounts", engine)
        known_accounts.update(new_external_receiver_accounts["account_number"])

    pdf = pdf[pdf["receiver_account"].isin(known_accounts)].copy()

    transactions_df = pdf[[
        "transaction_id", "timestamp", "amount", "currency", "sender_account",
        "receiver_account", "transaction_type", "id", "notes"
    ]].rename(columns={"id": "location_id"}).drop_duplicates("transaction_id")

    transactions_df.drop(columns=["is_internal_sender"], errors="ignore", inplace=True)

    transactions_df = tag_dataframe(transactions_df, MIGRATION_RUN_ID)
    batch_insert(transactions_df, "transactions", engine, chunk_size=500)

if __name__ == "__main__":
    print(f"🚀 Startar ETL, run ID: {MIGRATION_RUN_ID}")

    try:
        print("Laddar kunder och konton...")
        load_customers_accounts("data/sebank_customers_with_accounts.csv")
        print("Laddar transaktioner...")
        load_transactions("data/transactions.csv")

        session.add(MigrationRun(id=MIGRATION_RUN_ID, description="Batch ETL import"))
        session.commit()

        alembic_cfg = Config("alembic.ini")
        create_alembic_revision_if_data_exists(alembic_cfg, "Batch ETL import", MIGRATION_RUN_ID, session)

        print("✅ Dataimport färdig.")

        print("\n📊 Felstatistik:")
        for reason, count in error_counts.items():
            print(f" - {reason}: {count} st")

    except Exception as e:
        session.rollback()
        print(f"❌ Fel under ETL: {e}")
        raise
