import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from models import Base  # Importera Base och alla modeller

# Ladda .env
load_dotenv(dotenv_path="/Users/thomasrosen/Documents/Dev/DATA24STO/Datakvalitet/Bank_Projekt/.env")
DATABASE_URL = os.getenv("DATABASE_URL")

# Skapa engine
engine = create_engine(DATABASE_URL)

# Skapa alla tabeller
if __name__ == "__main__":
    Base.metadata.drop_all(engine)   # 💣 Ta bort befintliga tabeller (om du vill börja om)
    Base.metadata.create_all(engine)
    print("✅ Databasen har initierats från models.py")
