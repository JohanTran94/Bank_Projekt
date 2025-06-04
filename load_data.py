from collections import defaultdict
from datetime import datetime
from pathlib import Path
import uuid
import json
import csv
import os
import re

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from alembic.config import Config
import dask.dataframe as dd
from dotenv import load_dotenv
from alembic import command
import pandas as pd
from tqdm import tqdm

from models import ErrorRow, MigrationRun


def preprocess_ssn(ssn: str) -> str:
    if not isinstance(ssn, str):
        return ""
    ssn = ssn.strip()


    if re.match(r'^\d{10}$', ssn):
        return ssn[:6] + "-" + ssn[6:]
    elif re.match(r'^\d{12}$', ssn):
        return ssn[:8] + "-" + ssn[8:]
    return ssn


def is_valid_ssn(ssn: str) -> bool:
    if not isinstance(ssn, str):
        return False
    ssn = preprocess_ssn(ssn)
    if not ssn:
        return False
    return bool(re.match(r'^(\d{6}|\d{8})[-+]\d{4}$', ssn))



def preprocess_phone(phone: str) -> str:
    if not isinstance(phone, str):
        return ""
    phone = phone.strip()

    # Remove all spaces, parentheses, dashes
    phone = re.sub(r'[ \-\(\)]', '', phone)

    # Remove leading zero if after country code e.g. '+460396...' → '+46396...'
    phone = re.sub(r'^\+460', '+46', phone)

    # If phone starts with '0' (local format), replace leading '0' with '+46'
    if phone.startswith('0'):
        phone = '+46' + phone[1:]

    # If phone does not start with +46, add it (assume local)
    if not phone.startswith('+46'):
        # Remove leading zeros first
        phone = phone.lstrip('0')
        phone = '+46' + phone

    return phone

def is_valid_phone(phone: str) -> bool:
    if not isinstance(phone, str):
        return False
    return re.match(r'^\+46\d{7,10}$', phone) is not None


def preprocess_iban(iban: str) -> str:
    if not isinstance(iban, str):
        return ""
    iban = iban.strip().replace(' ', '').upper()
    return iban

def is_valid_iban(iban: str) -> bool:
    if not isinstance(iban, str):
        return False
    return re.match(r'^SE\d{2}[A-Z0-9]{16,20}$', iban) is not None


def detect_csv_type(csv_path: Path) -> str:
    """
    Inspect the header row of a CSV file to determine its data type.

    Parameters:
        csv_path (Path): The path to the CSV file to inspect.

    Returns:
        str: A string indicating the file type:
             - 'customers_accounts' if the CSV matches the customer/account schema,
             - 'transactions' if it matches the transactions schema,
             - 'unknown' otherwise.
    """

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # Read the first line as header

    # Normalize headers to lowercase and strip whitespace for robust comparison
    headers_lower = [h.strip().lower() for h in headers]
    headers_set = set(headers_lower)

    # Define expected columns for customer/account files
    customer_cols = {"customer", "address", "phone", "personnummer", "bankaccount"}

    # Define expected columns for transaction files
    transaction_cols = {
        "transaction_id", "timestamp", "amount", "currency", "sender_account", "receiver_account",
        "sender_country", "sender_municipality", "receiver_country", "receiver_municipality",
        "transaction_type", "notes"
    }

    # Check if the header contains all required columns for customers/accounts
    if customer_cols.issubset(headers_set):
        return "customers_accounts"

    # Check if the header contains all required columns for transactions
    elif transaction_cols.issubset(headers_set):
        return "transactions"

    else:
        print("🔴 Unknown CSV structure: headers do not match expected schemas.")
        return "unknown"


MIGRATION_RUN_ID = uuid.uuid4()
error_counts = defaultdict(int)

def tag_dataframe(df, run_id):
    df["migration_run_id"] = str(run_id)
    return df

def create_alembic_revision_if_data_exists(alembic_cfg, description: str, run_id: uuid.UUID, session):
    from sqlalchemy import text

    version_dir = Path("alembic/versions")
    short_run_id = str(run_id)[:8]

    # Look for existing revision files that contain the short run_id in their content
    for file_path in version_dir.glob("*.py"):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            if short_run_id in content:
                print(f"🟡 Alembic revision for run_id {short_run_id} already exists in {file_path.name}. Skipping.")
                return

    # Check if data with this run_id exists in tables
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
        print("🟡 No data with this run_id found – skipping Alembic revision.")
        return

    # Create new Alembic revision file
    message = f"ETL {short_run_id}"
    command.revision(alembic_cfg, message=message, autogenerate=False)

    # Find the newest revision file (just created)
    revision_files = list(version_dir.glob("*.py"))
    revision_path = max(revision_files, key=os.path.getmtime)

    # Insert run_id inside the revision file (after first docstring)
    with open(revision_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for i, line in enumerate(lines):
        if '"""' in line:
            lines.insert(i + 1, f"Run ID: {run_id}\n")
            break

    with open(revision_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    # Append upgrade/downgrade functions with delete statements filtered by run_id
    content = f"""

def upgrade():
    print("Data load completed externally. Run ID: {run_id}")

def downgrade():
    from alembic import op
    conn = op.get_bind()
    conn.execute("DELETE FROM transactions WHERE migration_run_id = '{run_id}'")
    conn.execute("DELETE FROM accounts WHERE migration_run_id = '{run_id}'")
    conn.execute("DELETE FROM customers WHERE migration_run_id = '{run_id}'")
    conn.execute("DELETE FROM transaction_locations WHERE migration_run_id = '{run_id}'")
"""

    with open(revision_path, "a", encoding="utf-8") as f:
        f.write(content)

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
    total_batches = (len(df) + chunk_size - 1) // chunk_size

    for start in tqdm(range(0, len(df), chunk_size), desc=f"Inserting into {table}", unit="batch"):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(table, con=engine, if_exists="append", index=False, method="multi")
        except IntegrityError as e:
            print(f"\n🔴 Error during batch_insert into table '{table}': {e}\n")
            if session:
                session.rollback()


def load_customers_accounts(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns={
        "customer": "name",
        "address": "address",
        "phone": "phone",
        "personnummer": "ssn",
        "bankaccount": "account_number"
    })

    required_fields = ["name", "ssn", "account_number"]
    invalid_rows = df[df[required_fields].isnull().any(axis=1)]
    valid_rows = df.dropna(subset=required_fields)

    pdf = valid_rows.compute()

    for _, row in invalid_rows.compute().iterrows():
        log_error_row("load_customers_accounts", "Missing required fields", row.to_dict(), "invalid_customers.csv")

    # Validate and preprocess SSN
    pdf["ssn"] = pdf["ssn"].apply(preprocess_ssn)
    invalid_ssn = pdf[~pdf["ssn"].apply(is_valid_ssn)]
    for _, row in invalid_ssn.iterrows():
        log_error_row("load_customers_accounts", "Invalid SSN format", row.to_dict(), "invalid_ssn.csv")
    pdf = pdf[pdf["ssn"].apply(is_valid_ssn)]

    # Validate and preprocess phone numbers
    pdf["phone"] = pdf["phone"].apply(preprocess_phone)
    invalid_phone = pdf[~pdf["phone"].apply(is_valid_phone)]
    for _, row in invalid_phone.iterrows():
        log_error_row("load_customers_accounts", "Invalid phone number format", row.to_dict(), "invalid_phone.csv")
    pdf = pdf[pdf["phone"].apply(is_valid_phone)]

    # Check that 'account_number' column exists before IBAN processing
    if "account_number" not in pdf.columns:
        raise KeyError("account_number column missing before IBAN preprocessing")

    # Preprocess IBAN once
    pdf["account_number"] = pdf["account_number"].apply(preprocess_iban)

    # Validate IBAN
    invalid_iban = pdf[~pdf["account_number"].apply(is_valid_iban)]
    for _, row in invalid_iban.iterrows():
        log_error_row("load_customers_accounts", "Invalid IBAN format", row.to_dict(), "invalid_iban.csv")
    pdf = pdf[pdf["account_number"].apply(is_valid_iban)]

    # De-duplicate and insert customers
    unique_customers = pdf.drop_duplicates(subset="ssn").copy()

    # Failsafe check
    missing_cols = [col for col in ["name", "address", "phone", "ssn"] if col not in unique_customers.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in unique_customers: {missing_cols}")

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

    valid_currencies = {"SEK", "NOK", "DKK", "JPY", "USD", "EUR", "GBP", "CNY", "RMB"}
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
    print(f"🟡 Starting ETL process, run ID: {MIGRATION_RUN_ID}")

    try:
        data_path = Path("data")
        csv_files = sorted(data_path.glob("*.csv"))

        for csv_file in csv_files:
            print(f"\n🟡 Inspecting file: {csv_file.name}")
            file_type = detect_csv_type(csv_file)

            if file_type == "customers_accounts":
                print(f"🟡 Loading customers and accounts from {csv_file.name}...")
                load_customers_accounts(str(csv_file))

            elif file_type == "transactions":
                print(f"🟡 Loading transactions from {csv_file.name}...")
                load_transactions(str(csv_file))

            else:
                print(f"🔴️ Unknown CSV structure in file {csv_file.name}. Skipping this file.")

        session.add(MigrationRun(id=MIGRATION_RUN_ID, description="Batch ETL import"))
        session.commit()

        alembic_cfg = Config("alembic.ini")
        create_alembic_revision_if_data_exists(alembic_cfg, "Batch ETL import", MIGRATION_RUN_ID, session)

        print("🟢 Data import complete.")

        print("\n🔵 Error statistics:")
        for reason, count in error_counts.items():
            print(f" - {reason}: {count} entries")

    except Exception as e:
        session.rollback()
        print(f"🔴 Error during ETL process: {e}")
        raise

