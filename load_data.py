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

os.chdir(Path(__file__).resolve().parent)
load_dotenv()

# Ladda miljövariabler
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

    # Logga till databas
    error = ErrorRow(
        context=context,
        error_reason=reason,
        raw_data=clean_row,
        logged_at=datetime.utcnow()
    )
    session.add(error)
    session.commit()

    # Logga till CSV
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


# ------------- 🧱 batch_insert ----------------
def batch_insert(df: pd.DataFrame, table: str, engine, chunk_size=100):
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(table, con=engine, if_exists="append", index=False, method="multi")
        except IntegrityError as e:
            print(f"⚠️  Skippar batch pga dubblettfel i tabell '{table}': {e}")

# ------------- 👥 load_customers_accounts ----------------
def load_customers_accounts(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    df = df.rename(columns={
        "Customer": "name",
        "Address": "address",
        "Phone": "phone",
        "Personnummer": "ssn",
        "BankAccount": "account_number"
    })

    # 🚫 Hitta rader med saknade nyckelfält
    required_fields = ["name", "ssn", "account_number"]
    invalid_rows = df[df[required_fields].isnull().any(axis=1)]
    valid_rows = df.dropna(subset=required_fields)

    # 🪵 Logga felrader
    for _, row in invalid_rows.compute().iterrows():
        log_error_row(
            context="load_customers_accounts",
            reason="Missing required fields: name, ssn, or account_number",
            row=row.to_dict(),
            csv_file="invalid_customers.csv"
        )

    # 🧠 Gå vidare med korrekta rader
    pdf = valid_rows.compute()

    # ✅ Ladda unika kunder
    unique_customers = pdf.drop_duplicates(subset="ssn").copy()
    batch_insert(unique_customers[["name", "address", "phone", "ssn"]], "customers", engine)

    # 🔗 Hämta customer-id från databas
    with engine.connect() as conn:
        customer_map = pd.read_sql("SELECT id, ssn FROM customers", conn)

    # 🔄 Koppla konton till kunder
    merged = pdf.merge(customer_map, on="ssn", how="left")
    accounts_df = merged[["account_number", "id"]].drop_duplicates("account_number")
    accounts_df = accounts_df.rename(columns={"id": "customer_id"})

    batch_insert(accounts_df, "accounts", engine)


# ------------- 🧠 ensure_transaction_locations_exist ----------------
def ensure_transaction_locations_exist(pdf: pd.DataFrame) -> pd.DataFrame:
    location_cols = [
        "sender_country", "sender_municipality", "receiver_country", "receiver_municipality"
    ]
    unique_locs = pdf[location_cols].drop_duplicates().copy()

    with engine.connect() as conn:
        existing = pd.read_sql("SELECT * FROM transaction_locations", conn)

    frames = [df.dropna(how="all") for df in [existing, unique_locs]]
    combined = pd.concat(frames).drop_duplicates(location_cols, keep="first")
    combined = combined.reset_index(drop=True)
    combined["id"] = combined.index + 1

    new_locs = combined[~combined["id"].isin(existing["id"])]
    if not new_locs.empty:
        batch_insert(new_locs, "transaction_locations", engine)

    merged = pdf.merge(combined, on=location_cols, how="left")
    return merged


# ------------- 💸 load_transactions ----------------
def load_transactions(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    # 🚫 Hitta rader med saknade obligatoriska fält
    required_fields = ["transaction_id", "sender_account", "receiver_account"]
    missing_fields = df[df[required_fields].isnull().any(axis=1)]
    df = df.dropna(subset=required_fields)

    for _, row in missing_fields.compute().iterrows():
        log_error_row(
            context="load_transactions",
            reason="Missing required transaction fields",
            row=row.to_dict(),
            csv_file="invalid_transactions.csv"
        )

    # 🚫 Konvertera belopp (amount) till float
    df["amount"] = df["amount"].map_partitions(lambda s: pd.to_numeric(s, errors="coerce"))
    invalid_amounts = df[df["amount"].map_partitions(lambda x: pd.to_numeric(x, errors="coerce").isna())]
    df = df[df["amount"].map_partitions(lambda x: pd.to_numeric(x, errors="coerce").notna())]

    for _, row in invalid_amounts.compute().iterrows():
        log_error_row(
            context="load_transactions",
            reason="Invalid amount (not a number)",
            row=row.to_dict(),
            csv_file="invalid_amounts.csv"
        )

    # 🚫 Konvertera datum
    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    invalid_dates = df[df["timestamp"].isna()]
    df = df.dropna(subset=["timestamp"])

    for _, row in invalid_dates.compute().iterrows():
        log_error_row(
            context="load_transactions",
            reason="Invalid timestamp (could not parse date)",
            row=row.to_dict(),
            csv_file="invalid_timestamps.csv"
        )

    # ✅ Konvertera till pandas
    pdf = df.compute()

    # 🔗 Mappa location_id
    pdf = ensure_transaction_locations_exist(pdf)

    # 🧠 Markera interna konton
    pdf["is_internal_sender"] = pdf["sender_account"].str.startswith("SE")

    # 🔍 Kolla befintliga konton
    with engine.connect() as conn:
        existing_accounts = pd.read_sql("SELECT account_number FROM accounts", conn)
    known_accounts = set(existing_accounts["account_number"])

    unknown_senders = pdf[~pdf["sender_account"].isin(known_accounts)]

    # 🚫 Interna konton utan match → loggas & utesluts
    internal_missing = unknown_senders[unknown_senders["is_internal_sender"]]
    for _, row in internal_missing.iterrows():
        log_error_row(
            context="load_transactions",
            reason="Internal sender account missing in accounts table",
            row=row.to_dict(),
            csv_file="internal_missing_accounts.csv"
        )

    # ✅ Externa konton → skapas utan customer_id
    external_missing = unknown_senders[~unknown_senders["is_internal_sender"]]
    if not external_missing.empty:
        new_external_accounts = pd.DataFrame({
            "account_number": external_missing["sender_account"].unique(),
            "customer_id": None
        })
        batch_insert(new_external_accounts, "accounts", engine)
        known_accounts.update(new_external_accounts["account_number"])

    # 🧹 Filtrera bort felaktiga interna
    pdf = pdf[pdf["sender_account"].isin(known_accounts)].copy()

    # 📦 Förbered transaktioner
    transactions_df = pdf[[
        "transaction_id", "timestamp", "amount", "currency", "sender_account",
        "receiver_account", "transaction_type", "id", "notes"
    ]].rename(columns={"id": "location_id"}).drop_duplicates("transaction_id")

    # 🧼 Ta bort hjälpkolumn
    transactions_df.drop(columns=["is_internal_sender"], errors="ignore", inplace=True)

    batch_insert(transactions_df, "transactions", engine, chunk_size=100)




# ------------- 🚀 main ----------------
if __name__ == "__main__":
    print("Laddar kunder och konton...")
    load_customers_accounts("data/sebank_customers_with_accounts.csv")

    print("Laddar transaktioner...")
    load_transactions("data/transactions.csv")

    print("✅ Dataimport färdig.")
