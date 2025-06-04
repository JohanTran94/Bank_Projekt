import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from pathlib import Path

# Set working directory and load environment variables
os.chdir(Path(__file__).resolve().parent)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

try:
    engine = create_engine(DATABASE_URL)
    print("🟢 Successfully connected to the database")
except SQLAlchemyError as e:
    print(f"🔴 Failed to connect to the database: {e}")
    exit(1)

# Import models after setting up the engine
from models import Base
from models import Customer, Account, TransactionLocation, Transaction  # Important!

if __name__ == "__main__":
    try:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        print("🟢 Database initialized from models.py")
    except SQLAlchemyError as e:
        print(f"🔴 Failed to initialize the database: {e}")
        exit(1)

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS alembic_version;"))
            conn.execute(text("""
                CREATE TABLE alembic_version (
                    version_num TEXT NOT NULL PRIMARY KEY
                );
            """))
            print("🟢 'alembic_version' table created successfully")
    except SQLAlchemyError as e:
        print(f"🔴 Failed to create 'alembic_version' table: {e}")
        exit(1)

