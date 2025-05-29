import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
os.chdir(Path(__file__).resolve().parent)
load_dotenv()

# Ladda miljövariabler
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Skapa engine
engine = create_engine(DATABASE_URL)

# Importera modeller och Base
from models import Base
from models import Customer, Account, TransactionLocation, Transaction  # 🧠 Detta är avgörande!

# Skapa alla tabeller
if __name__ == "__main__":
    Base.metadata.drop_all(engine)   # 💣 Tar bort tabeller (valfritt men bra i början)
    Base.metadata.create_all(engine)
    print("✅ Databasen har initierats från models.py")

