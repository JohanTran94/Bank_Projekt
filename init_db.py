import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

os.chdir(Path(__file__).resolve().parent)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

from models import Base
from models import Customer, Account, TransactionLocation, Transaction  # viktigt!

if __name__ == "__main__":
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("✅ Databasen har initierats från models.py")

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS alembic_version;"))
        conn.execute(text("""
            CREATE TABLE alembic_version (
                version_num TEXT NOT NULL PRIMARY KEY
            );
        """))
