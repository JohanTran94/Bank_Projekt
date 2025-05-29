import os
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv

# Ladda miljövariabler
load_dotenv(dotenv_path="/Users/thomasrosen/Documents/Dev/DATA24STO/Datakvalitet/Bank_Projekt/.env")
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# ------------- 🧱 batch_insert ----------------
def batch_insert(df: pd.DataFrame, table: str, engine, chunk_size=1000):
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

    df = df.dropna(subset=["name", "ssn", "account_number"])
    pdf = df.compute()

    unique_customers = pdf.drop_duplicates(subset="ssn").copy()
    batch_insert(unique_customers[["name", "address", "phone", "ssn"]], "customers", engine)

    with engine.connect() as conn:
        customer_map = pd.read_sql("SELECT id, ssn FROM customers", conn)

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

    # Fix: undvik FutureWarning
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

    df["amount"] = df["amount"].astype(float)
    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["transaction_id", "sender_account", "receiver_account"])
    pdf = df.compute()

    # ✅ Se till att locations finns och mappa till location_id
    pdf = ensure_transaction_locations_exist(pdf)

    # 🚫 Filter: Ta bort transaktioner där sender_account inte finns
    with engine.connect() as conn:
        existing_accounts = pd.read_sql("SELECT account_number FROM accounts", conn)
    known_senders = set(existing_accounts["account_number"])
    filtered = pdf[pdf["sender_account"].isin(known_senders)].copy()

    # 📦 Välj kolumner och döp om
    transactions_df = filtered[[
        "transaction_id", "timestamp", "amount", "currency", "sender_account",
        "receiver_account", "transaction_type", "id", "notes"
    ]].rename(columns={"id": "location_id"}).drop_duplicates("transaction_id")

    batch_insert(transactions_df, "transactions", engine)

# ------------- 🚀 main ----------------
if __name__ == "__main__":
    print("Laddar kunder och konton...")
    load_customers_accounts("data/sebank_customers_with_accounts.csv")

    print("Laddar transaktioner...")
    load_transactions("data/transactions.csv")

    print("✅ Dataimport färdig.")
