from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, Text, JSON, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    address = Column(Text)
    phone = Column(Text)
    ssn = Column(Text, unique=True, nullable=False)  # unikt personnummer

    accounts = relationship("Account", back_populates="customer")


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True)
    account_number = Column(Text, unique=True, nullable=False)  # unikt kontonummer
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False, index=True)

    customer = relationship("Customer", back_populates="accounts")


class TransactionLocation(Base):
    __tablename__ = "transaction_locations"

    id = Column(Integer, primary_key=True)
    sender_country = Column(Text, nullable=False)
    sender_municipality = Column(Text, nullable=False)
    receiver_country = Column(Text, nullable=False)
    receiver_municipality = Column(Text, nullable=False)

    # ENDAST om du vill undvika exakt dubblett av samma platspar
    __table_args__ = (
        UniqueConstraint(
            "sender_country", "sender_municipality", "receiver_country", "receiver_municipality",
            name="uq_transaction_location"
        ),
    )


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True)  # unikt ID från CSV
    timestamp = Column(DateTime, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, nullable=False)
    sender_account = Column(String, ForeignKey("accounts.account_number"), nullable=False)
    receiver_account = Column(String, nullable=False)
    transaction_type = Column(String, nullable=False)
    location_id = Column(Integer, ForeignKey("transaction_locations.id"), nullable=False)
    notes = Column(String)


class ErrorRow(Base):
    __tablename__ = "error_rows"

    id = Column(Integer, primary_key=True)
    context = Column(Text, nullable=False)
    error_reason = Column(Text, nullable=False)
    raw_data = Column(JSON, nullable=False)
    logged_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<ErrorRow(context='{self.context}', reason='{self.error_reason}')>"
