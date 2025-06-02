import os
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

from models import ErrorRow
from error_logger import log_error_row

os.chdir(Path(__file__).resolve().parent)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def batch_insert(df: pd.DataFrame, table: str, engine, chunk_size=100):
    if df.empty:
        return

    conn = engine.raw_connection()
    cursor = conn.cursor()

    cols = list(df.columns)
    values = df.values.tolist()

    insert_stmt = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"

    for start in range(0, len(values), chunk_size):
        chunk = values[start:start + chunk_size]
        try:
            execute_values(cursor, insert_stmt, chunk)
            conn.commit()
        except psycopg2.errors.UniqueViolation as e:
            conn.rollback()
            print(f"⚠️  Skippar batch på grund av dubblettfel i tabell '{table}': {e}")
        except Exception as e:
            conn.rollback()
            print(f"Fel vid batch_insert i tabell '{table}': {e}")

    cursor.close()
    conn.close()


def load_customers_accounts(csv_path: str):
    session = Session()
    try:
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
            log_error_row(
                session,
                context="load_customers_accounts",
                reason="Missing required fields: name, ssn, or account_number",
                row=row.to_dict(),
                csv_file="invalid_customers.csv"
            )

        pdf = valid_rows.compute()

        unique_customers = pdf.drop_duplicates(subset="ssn").copy()
        batch_insert(unique_customers[["name", "address", "phone", "ssn"]], "customers", engine)

        with engine.connect() as conn:
            customer_map = pd.read_sql("SELECT id, ssn FROM customers", conn)

        merged = pdf.merge(customer_map, on="ssn", how="left")
        accounts_df = merged[["account_number", "id"]].drop_duplicates("account_number")
        accounts_df = accounts_df.rename(columns={"id": "customer_id"})

        batch_insert(accounts_df, "accounts", engine)

    except Exception as e:
        print(f"Fel i load_customers_accounts: {e}")
    finally:
        session.close()


def ensure_transaction_locations_exist(pdf: pd.DataFrame) -> pd.DataFrame:
    location_cols = [
        "sender_country", "sender_municipality", "receiver_country", "receiver_municipality"
    ]
    unique_locs = pdf[location_cols].drop_duplicates().copy()

    with engine.connect() as conn:
        existing = pd.read_sql("SELECT * FROM transaction_locations", conn)

    frames = [existing, unique_locs]
    combined = pd.concat(frames).drop_duplicates(subset=location_cols, keep="first").reset_index(drop=True)

    # Nytt id-start = max befintliga id + 1 eller 1 om tomt
    max_id = existing["id"].max() if not existing.empty else 0
    combined = combined.reset_index(drop=True)
    combined["id"] = combined.index + 1 + max_id

    new_locs = combined[~combined["id"].isin(existing["id"])]
    if not new_locs.empty:
        batch_insert(new_locs, "transaction_locations", engine)

    merged = pdf.merge(combined, on=location_cols, how="left")
    return merged


def load_transactions(csv_path: str):
    session = Session()
    try:
        df = dd.read_csv(csv_path, dtype=str, assume_missing=True)

        required_fields = ["transaction_id", "sender_account", "receiver_account"]
        missing_fields = df[df[required_fields].isnull().any(axis=1)]
        df = df.dropna(subset=required_fields)

        for _, row in missing_fields.compute().iterrows():
            log_error_row(
                session,
                context="load_transactions",
                reason="Missing required transaction fields",
                row=row.to_dict(),
                csv_file="invalid_transactions.csv"
            )

        df["amount"] = df["amount"].map_partitions(lambda s: pd.to_numeric(s, errors="coerce"))
        invalid_amounts = df[df["amount"].map_partitions(lambda x: pd.to_numeric(x, errors="coerce").isna())]
        df = df[df["amount"].map_partitions(lambda x: pd.to_numeric(x, errors="coerce").notna())]

        for _, row in invalid_amounts.compute().iterrows():
            log_error_row(
                session,
                context="load_transactions",
                reason="Invalid amount (not a number)",
                row=row.to_dict(),
                csv_file="invalid_amounts.csv"
            )

        df["timestamp"] = dd.to_datetime(df["timestamp"], errors="coerce")
        invalid_dates = df[df["timestamp"].isna()]
        df = df.dropna(subset=["timestamp"])

        for _, row in invalid_dates.compute().iterrows():
            log_error_row(
                session,
                context="load_transactions",
                reason="Invalid timestamp (could not parse date)",
                row=row.to_dict(),
                csv_file="invalid_timestamps.csv"
            )

        pdf = df.compute()

        # Uppdatera locations med ids
        pdf = ensure_transaction_locations_exist(pdf)

        # Markera interna konton (exempelvis börjar med 'SE')
        pdf["is_internal_sender"] = pdf["sender_account"].str.startswith("SE")

        with engine.connect() as conn:
            existing_accounts = pd.read_sql("SELECT account_number FROM accounts", conn)
        known_accounts = set(existing_accounts["account_number"])

        unknown_senders = pdf[~pdf["sender_account"].isin(known_accounts)]

        # Logga och filtrera interna konton utan match
        internal_missing = unknown_senders[unknown_senders["is_internal_sender"]]
        for _, row in internal_missing.iterrows():
            log_error_row(
                session,
                context="load_transactions",
                reason="Internal sender account missing in accounts table",
                row=row.to_dict(),
                csv_file="internal_missing_accounts.csv"
            )

        # Skapa nya externa konton utan customer_id
        external_missing = unknown_senders[~unknown_senders["is_internal_sender"]]
        if not external_missing.empty:
            new_external_accounts = pd.DataFrame({
                "account_number": external_missing["sender_account"].unique(),
                "customer_id": None
            })
            batch_insert(new_external_accounts, "accounts", engine)
            known_accounts.update(new_external_accounts["account_number"])

        # Filtrera bort felaktiga interna
        pdf = pdf[pdf["sender_account"].isin(known_accounts)].copy()

        transactions_df = pdf[[
            "transaction_id", "timestamp", "amount", "currency", "sender_account",
            "receiver_account", "transaction_type", "id", "notes"
        ]].rename(columns={"id": "location_id"}).drop_duplicates("transaction_id")

        transactions_df.drop(columns=["is_internal_sender"], errors="ignore", inplace=True)

        batch_insert(transactions_df, "transactions", engine, chunk_size=100)

    except Exception as e:
        print(f"Fel i load_transactions: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    print("Laddar kunder och konton...")
    load_customers_accounts("data/sebank_customers_with_accounts.csv")

    print("Laddar transaktioner...")
    load_transactions("data/transactions.csv")

    print("✅ Dataimport färdig.")
