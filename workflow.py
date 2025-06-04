from prefect import flow, task, get_run_logger
from load_data import load_customers_accounts, load_transactions
from models import Base
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv
from pathlib import Path

# Initialize .env and database connection
os.chdir(Path(__file__).resolve().parent)
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


@task
def create_tables():
    logger = get_run_logger()
    try:
        logger.info("Creating tables...")
        Base.metadata.create_all(engine)
        logger.info("🟢 Tables created successfully")
    except SQLAlchemyError as e:
        logger.error(f"🔴 Error while creating tables: {e}")
        raise


@task
def load_customers(csv_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"Reading customers from {csv_path}")
        load_customers_accounts(csv_path)
        logger.info("🟢 Customers & accounts loaded successfully")
    except Exception as e:
        logger.error(f"🔴 Error while loading customers: {e}")
        raise


@task
def load_transactions_task(csv_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"Reading transactions from {csv_path}")
        load_transactions(csv_path)
        logger.info("🟢 Transactions loaded successfully")
    except Exception as e:
        logger.error(f"🔴 Error while loading transactions: {e}")
        raise


@flow(name="Populate Bank Database")
def populate_normalized_database(
        customer_csv: str = "data/sebank_customers_with_accounts.csv",
        transaction_csv: str = "data/transactions.csv"
):
    logger = get_run_logger()
    logger.info("Starting Prefect flow for data import")

    create_tables()
    load_customers(customer_csv)
    load_transactions_task(transaction_csv)

    logger.info("🟢 Prefect flow completed successfully")


if __name__ == "__main__":
    populate_normalized_database()
