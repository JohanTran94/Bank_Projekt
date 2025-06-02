import os
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
from models import ErrorRow
from dotenv import load_dotenv
from pathlib import Path

# 📦 Miljövariabler och databas
os.chdir(Path(__file__).resolve().parent)
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# 🔧 Hjälpfunktioner
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
    print(f"🔴 log_error_row TRIGGAD | {context} | {reason}")  # DEBUG
    clean_row = serialize_for_json(row)
    error = ErrorRow(
        context=context,
        error_reason=reason,
        raw_data=clean_row,
        logged_at=datetime.utcnow()
    )
    session.add(error)
    session.commit()

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

def batch_insert(df: pd.DataFrame, table: str, engine, chunk_size=100, session=None):
    if df.empty:
        return

    df = df.where(pd.notna(df), None)

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(table, con=engine, if_exists="append", index=False, method="multi")
        except Exception as e:
            print(f"\nFel vid batch_insert i tabell '{table}': {e}\n")
            if session:
                session.rollback()

            for _, row in chunk.iterrows():
                try:
                    pd.DataFrame([row]).to_sql(table, con=engine, if_exists="append", index=False)
                except Exception as e_row:
                    if session:
                        session.rollback()
                    log_error_row(
                        context=f"batch_insert_{table}",
                        reason=str(e_row).splitlines()[0],
                        row=row.to_dict(),
                        csv_file=f"errors_{table}.csv"
                    )

# 🧩 Ladda kunder & konton
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
    batch_insert(unique_customers[["name", "address", "phone", "ssn"]], "customers", engine, session=session)

    with engine.connect() as conn:
        customer_map = pd.read_sql("SELECT id, ssn FROM customers", conn)

    merged = pdf.merge(customer_map, on="ssn", how="left")
    accounts_df = merged[["account_number", "id"]].drop_duplicates("account_number")
    accounts_df = accounts_df.rename(columns={"id": "customer_id"})

    batch_insert(accounts_df, "accounts", engine, session=session)

# 📍 Se till att platser finns i tabellen
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
        batch_insert(new_locs, "transaction_locations", engine, session=session)

    merged = pdf.merge(combined, on=location_cols, how="left")
    return merged

# 💳 Ladda transaktioner
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
        log_error_row("load_transactions", "Invalid amount", row.to_dict(), "invalid_amounts.csv")

    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    invalid_dates = df[df["timestamp"].isna()]
    df = df.dropna(subset=["timestamp"])

    for _, row in invalid_dates.compute().iterrows():
        log_error_row("load_transactions", "Invalid timestamp", row.to_dict(), "invalid_timestamps.csv")

    pdf = df.compute()
    pdf = ensure_transaction_locations_exist(pdf)

    pdf["is_internal_sender"] = pdf["sender_account"].str.startswith("SE")

    with engine.connect() as conn:
        existing_accounts = pd.read_sql("SELECT account_number FROM accounts", conn)
    known_accounts = set(existing_accounts["account_number"])

    unknown_senders = pdf[~pdf["sender_account"].isin(known_accounts)]
    internal_missing = unknown_senders[unknown_senders["is_internal_sender"]]
    for _, row in internal_missing.iterrows():
        log_error_row("load_transactions", "Internal sender missing", row.to_dict(), "internal_missing_accounts.csv")

    external_missing = unknown_senders[~unknown_senders["is_internal_sender"]]
    if not external_missing.empty:
        new_external = pd.DataFrame({
            "account_number": external_missing["sender_account"].unique(),
            "customer_id": None
        })
        batch_insert(new_external, "accounts", engine)
        known_accounts.update(new_external["account_number"])

    pdf = pdf[pdf["sender_account"].isin(known_accounts)].copy()

    unknown_receivers = pdf[~pdf["receiver_account"].isin(known_accounts)]
    internal_receivers = unknown_receivers[unknown_receivers["receiver_account"].str.startswith("SE")]
    for _, row in internal_receivers.iterrows():
        log_error_row("load_transactions", "Internal receiver missing", row.to_dict(), "internal_missing_receivers.csv")

    external_receivers = unknown_receivers[~unknown_receivers["receiver_account"].str.startswith("SE")]
    if not external_receivers.empty:
        new_external_receiver = pd.DataFrame({
            "account_number": external_receivers["receiver_account"].unique(),
            "customer_id": None
        })
        batch_insert(new_external_receiver, "accounts", engine)
        known_accounts.update(new_external_receiver["account_number"])

    pdf = pdf[pdf["receiver_account"].isin(known_accounts)].copy()

    transactions_df = pdf[[
        "transaction_id", "timestamp", "amount", "currency", "sender_account",
        "receiver_account", "transaction_type", "id", "notes"
    ]].rename(columns={"id": "location_id"}).drop_duplicates("transaction_id")

    transactions_df.drop(columns=["is_internal_sender"], errors="ignore", inplace=True)
    batch_insert(transactions_df, "transactions", engine, chunk_size=100)

# 🟢 Körs som script
if __name__ == "__main__":
    print("Laddar kunder och konton...")
    load_customers_accounts("data/sebank_customers_with_accounts.csv")

    print("Laddar transaktioner...")
    load_transactions("data/transactions.csv")

    print("✅ Dataimport färdig.")
