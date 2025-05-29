import os
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment
load_dotenv(dotenv_path="/Users/thomasrosen/Documents/Dev/DATA24STO/Datakvalitet/Bank_Projekt/.env")
DATABASE_URL = os.getenv("DATABASE_URL")

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# ------------- HELPERS ----------------

def batch_insert(df: pd.DataFrame, table: str, engine, chunk_size=1000):
    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        chunk = df.iloc[start:end]
        chunk.to_sql(table, con=engine, if_exists="append", index=False, method="multi")

# ------------- CUSTOMERS & ACCOUNTS ----------------

def load_customers_accounts(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    # Drop rows missing critical fields
    df = df.dropna(subset=["id", "name", "ssn", "account_number"])

    # Convert Dask to Pandas
    pdf = df.compute()

    # Insert into customers
    customers_df = pdf[["id", "name", "address", "phone", "ssn"]].drop_duplicates("ssn")
    customers_df["id"] = customers_df["id"].astype(int)
    batch_insert(customers_df, "customers", engine)

    # Insert into accounts
    accounts_df = pdf[["account_number", "id"]].rename(columns={"id": "customer_id"})
    batch_insert(accounts_df, "accounts", engine)

# ------------- TRANSACTIONS ----------------

def load_transactions(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    # Cleanup and type conversion
    df["amount"] = df["amount"].astype(float)
    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["transaction_id", "sender_account", "receiver_account"])

    # Convert to Pandas
    pdf = df.compute()

    # Create or match location IDs (simplified)
    location_cols = [
        "sender_country", "sender_municipality", "receiver_country", "receiver_municipality"
    ]
    locations = pdf[location_cols].drop_duplicates().copy()
    locations["id"] = range(1, len(locations)+1)
    batch_insert(locations, "transaction_locations", engine)

    # Merge to get location_id
    pdf = pdf.merge(locations, on=location_cols, how="left")

    # Prepare final transaction DataFrame
    transactions_df = pdf[[
        "transaction_id", "timestamp", "amount", "currency", "sender_account",
        "receiver_account", "transaction_type", "id", "notes"
    ]].rename(columns={"id": "location_id"})

    batch_insert(transactions_df, "transactions", engine)

# ------------- MAIN ----------------

if __name__ == "__main__":
    print("Laddar kunder och konton...")
    load_customers_accounts("data/sebank_customers_with_accounts.csv")

    print("Laddar transaktioner...")
    load_transactions("data/transactions.csv")

    print("✅ Dataimport färdig.")
