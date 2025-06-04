from prefect import flow, task, get_run_logger
from load_data import load_customers_accounts, load_transactions
from models import Base
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import os
import pandas as pd
from dotenv import load_dotenv

# Setup paths and environment
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
load_dotenv(BASE_DIR / ".env")
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# --- Utility Functions ---

def identify_csv_type(file_path: Path) -> str:
    try:
        df = pd.read_csv(file_path, nrows=1)
        columns = set(df.columns.str.lower())

        if {"customer", "personnummer", "bankaccount"}.issubset(columns):
            return "customers"
        elif {"transaction_id", "amount", "sender_account"}.issubset(columns):
            return "transactions"
        else:
            return "unknown"
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return "invalid"

def find_csv_files(data_dir: Path):
    customers_csv = None
    transactions_csv = None

    for file in data_dir.glob("*.csv"):
        file_type = identify_csv_type(file)
        if file_type == "customers":
            customers_csv = file
        elif file_type == "transactions":
            transactions_csv = file

    return customers_csv, transactions_csv

# --- Prefect Tasks ---

@task
def create_tables():
    logger = get_run_logger()
    try:
        logger.info("Creating tables...")
        Base.metadata.create_all(engine)
        logger.info("Tables created successfully")
    except SQLAlchemyError as e:
        logger.error(f"Error while creating tables: {e}")
        raise

@task
def load_customers(csv_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"Reading customers from {csv_path}")
        load_customers_accounts(csv_path)
        logger.info("Customers & accounts loaded successfully")
    except Exception as e:
        logger.error(f"Error while loading customers: {e}")
        raise

@task
def load_transactions_task(csv_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"Reading transactions from {csv_path}")
        load_transactions(csv_path)
        logger.info("Transactions loaded successfully")
    except Exception as e:
        logger.error(f"Error while loading transactions: {e}")
        raise

# --- Prefect Flow ---

@flow(name="Populate Bank Database")
def populate_normalized_database():
    logger = get_run_logger()
    logger.info("Searching for CSV files in data/")

    customers_csv, transactions_csv = find_csv_files(DATA_DIR)

    if not customers_csv:
        logger.error("Could not find a valid customer CSV file.")
        raise FileNotFoundError("Customer CSV file missing.")
    if not transactions_csv:
        logger.error("Could not find a valid transactions CSV file.")
        raise FileNotFoundError("Transactions CSV file missing.")

    logger.info(f" Found customer file: {customers_csv.name}")
    logger.info(f" Found transaction file: {transactions_csv.name}")

    create_tables()
    load_customers(str(customers_csv))
    load_transactions_task(str(transactions_csv))

    logger.info("Prefect flow completed successfully")

# --- Entry point ---

if __name__ == "__main__":
    populate_normalized_database()
