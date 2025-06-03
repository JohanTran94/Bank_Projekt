from prefect import flow, task
from load_data import load_customers_accounts, load_transactions
from models import Base
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path

# Ladda miljövariabler från .env
os.chdir(Path(__file__).resolve().parent)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

@task
def load_all_data():
    print("Kör kund- och kontoinläsning...")
    load_customers_accounts("data/sebank_customers_with_accounts.csv")

    print("Kör transaktionsinläsning...")
    load_transactions("data/transactions.csv")

@flow
def populate_normalized_database():
    print("🔁 Prefect-flow startar")
    load_all_data()
    print("✅ Prefect-flow klart")

if __name__ == "__main__":
    print(f"🔧 Skapar tabeller mot DB: {DATABASE_URL}")
    Base.metadata.create_all(engine)
    populate_normalized_database()
