from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, DateTime
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    address = Column(String)
    phone = Column(String)
    ssn = Column(String, unique=True, nullable=False)
    accounts = relationship("Account", back_populates="customer")

class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    account_number = Column(String, unique=True, nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    customer = relationship("Customer", back_populates="accounts")

class TransactionLocation(Base):
    __tablename__ = "transaction_locations"
    id = Column(Integer, primary_key=True)
    sender_country = Column(String)
    sender_municipality = Column(String)
    receiver_country = Column(String)
    receiver_municipality = Column(String)
    __table_args__ = (
        UniqueConstraint(
            "sender_country", "sender_municipality", "receiver_country", "receiver_municipality",
            name="uq_transaction_location"
        ),
    )

class Transaction(Base):
    __tablename__ = "transactions"
    transaction_id = Column(String, primary_key=True)
    timestamp = Column(DateTime)
    amount = Column(Float)
    currency = Column(String)
    sender_account = Column(String, ForeignKey("accounts.account_number"))
    receiver_account = Column(String, ForeignKey("accounts.account_number"))
    transaction_type = Column(String)  # ej FK längre, var utbruten i eget table. pointless
    location_id = Column(Integer, ForeignKey("transaction_locations.id"))
    notes = Column(String)
