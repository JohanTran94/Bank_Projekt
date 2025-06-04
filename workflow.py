from prefect import flow, task, get_run_logger
from load_data import load_customers_accounts, load_transactions
from models import Base
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv
from pathlib import Path

# Initiera .env och databasanslutning
os.chdir(Path(__file__).resolve().parent)
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


@task
def create_tables():
    logger = get_run_logger()
    try:
        logger.info("\033[94m️  Skapar tabeller...\033[0m")
        Base.metadata.create_all(engine)
        logger.info("\033[92m Tabeller skapade\033[0m")
    except SQLAlchemyError as e:
        logger.error(f"\033[91m Fel vid skapande av tabeller: {e}\033[0m")
        raise


@task
def load_customers(csv_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"\033[94m️  Läser in kunder från {csv_path}\033[0m")
        load_customers_accounts(csv_path)
        logger.info("\033[92m Kunder & konton inlästa\033[0m")
    except Exception as e:
        logger.error(f"\033[91m Fel vid kundinläsning: {e}\033[0m")
        raise


@task
def load_transactions_task(csv_path: str):
    logger = get_run_logger()
    try:
        logger.info(f"\033[94m️  Läser in transaktioner från {csv_path}\033[0m")
        load_transactions(csv_path)
        logger.info("\033[92m Transaktioner inlästa\033[0m")
    except Exception as e:
        logger.error(f"\033[91m Fel vid transaktionsinläsning: {e}\033[0m")
        raise


@flow(name="Populate Bank Database")
def populate_normalized_database(
    customer_csv: str = "data/sebank_customers_with_accounts.csv",
    transaction_csv: str = "data/transactions.csv"
):
    logger = get_run_logger()
    logger.info("\033[94m Startar Prefect-flow för datainläsning\033[0m")

    create_tables()
    load_customers(customer_csv)
    load_transactions_task(transaction_csv)

    logger.info("\033[92m Prefect-flow färdigt\033[0m")


if __name__ == "__main__":
    populate_normalized_database()
