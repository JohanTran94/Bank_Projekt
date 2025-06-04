from sqlalchemy import (
    Column, Integer, String, Float, ForeignKey, UniqueConstraint,
    DateTime, Text, JSON, Numeric, Index
)
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    address = Column(Text)
    phone = Column(Text)
    ssn = Column(Text, unique=True, nullable=False, index=True)

    migration_run_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    accounts = relationship("Account", back_populates="customer")


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True)
    account_number = Column(Text, unique=True, nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=True, index=True)

    migration_run_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    customer = relationship("Customer", back_populates="accounts")


class TransactionLocation(Base):
    __tablename__ = "transaction_locations"

    id = Column(Integer, primary_key=True)
    sender_country = Column(Text)
    sender_municipality = Column(Text)
    receiver_country = Column(Text)
    receiver_municipality = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            "sender_country", "sender_municipality",
            "receiver_country", "receiver_municipality",
            name="uq_transaction_location"
        ),
    )

    migration_run_id = Column(UUID(as_uuid=True), nullable=False, index=True)


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True)
    timestamp = Column(DateTime, index=True)
    amount = Column(Numeric(12, 2))  # exact representation for monetary values
    currency = Column(String(10))

    sender_account = Column(String, ForeignKey("accounts.account_number"), index=True)

    # Optional: also link receiver account if it may be internal
    receiver_account = Column(String, ForeignKey("accounts.account_number"), index=True)

    transaction_type = Column(String(50), index=True)
    location_id = Column(Integer, ForeignKey("transaction_locations.id"), index=True)
    notes = Column(String)

    migration_run_id = Column(UUID(as_uuid=True), nullable=False, index=True)


class ErrorRow(Base):
    __tablename__ = "error_rows"

    id = Column(Integer, primary_key=True)
    context = Column(Text, nullable=False)
    error_reason = Column(Text, nullable=False)
    raw_data = Column(JSON, nullable=False)
    logged_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<ErrorRow(context='{self.context}', reason='{self.error_reason}')>"


class MigrationRun(Base):
    __tablename__ = "migration_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
