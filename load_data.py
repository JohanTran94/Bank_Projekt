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

    # Byt kolumnnamn till standardiserade namn
    df = df.rename(columns={
        "Customer": "name",
        "Address": "address",
        "Phone": "phone",
        "Personnummer": "ssn",
        "BankAccount": "account_number"
    })

    # Ta bort rader med saknade viktiga fält
    df = df.dropna(subset=["name", "ssn", "account_number"])

    # Konvertera till Pandas
    pdf = df.compute()

    # Ta fram unika kunder
    unique_customers = pdf.drop_duplicates(subset="ssn").copy()
    unique_customers["id"] = range(1, len(unique_customers) + 1)

    # Skapa en mappning från ssn → id
    ssn_to_id = dict(zip(unique_customers["ssn"], unique_customers["id"]))

    # Lägg till customer_id i hela PDF baserat på ssn
    pdf["customer_id"] = pdf["ssn"].map(ssn_to_id)

    # Förbered customers-tabellen
    customers_df = unique_customers[["id", "name", "address", "phone", "ssn"]]
    batch_insert(customers_df, "customers", engine)

    # Förbered accounts-tabellen
    accounts_df = pdf[["account_number", "customer_id"]].drop_duplicates("account_number")
    batch_insert(accounts_df, "accounts", engine)

# ------------- TRANSACTIONS ----------------

def ensure_all_accounts_exist(pdf: pd.DataFrame):
    all_accounts = pd.concat([
        pdf["sender_account"],
        pdf["receiver_account"]
    ]).drop_duplicates().to_frame(name="account_number")

    all_accounts["customer_id"] = -1  # -1 = extern kund (ej registrerad)

    # Läs in redan existerande konton
    existing_accounts = pd.read_sql("SELECT account_number FROM accounts", engine)

    # Hitta saknade konton
    missing_accounts = all_accounts.loc[
        ~all_accounts["account_number"].isin(existing_accounts["account_number"])
    ]

    if not missing_accounts.empty:
        print(f"🔄 Lägger till {len(missing_accounts)} saknade konton från transaktioner...")
        batch_insert(missing_accounts, "accounts", engine)


def load_transactions(csv_path: str):
    df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

    print("Kolumner i CSV:", df.columns)

    # Cleanup and type conversion
    df["amount"] = df["amount"].astype(float)
    df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["transaction_id", "sender_account", "receiver_account"])

    # Convert to Pandas
    pdf = df.compute()

    # 🔁 Lägg till saknade konton i accounts-tabellen
    ensure_all_accounts_exist(pdf)

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
