from __future__ import annotations

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import Integer, String, DateTime, Text, JSON, Numeric
from datetime import datetime
from typing import Optional, List
import uuid


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[Optional[str]] = mapped_column(Text)
    phone: Mapped[Optional[str]] = mapped_column(Text)
    ssn: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)
    migration_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)

    accounts: Mapped[List[Account]] = relationship(back_populates="customer")


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    account_number: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id"), index=True)
    migration_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)

    customer: Mapped[Optional[Customer]] = relationship(back_populates="accounts")


class TransactionLocation(Base):
    __tablename__ = "transaction_locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    sender_country: Mapped[Optional[str]] = mapped_column(Text)
    sender_municipality: Mapped[Optional[str]] = mapped_column(Text)
    receiver_country: Mapped[Optional[str]] = mapped_column(Text)
    receiver_municipality: Mapped[Optional[str]] = mapped_column(Text)
    migration_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint(
            "sender_country", "sender_municipality",
            "receiver_country", "receiver_municipality",
            name="uq_transaction_location"
        ),
    )


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[str] = mapped_column(String, primary_key=True)
    timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)
    amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    currency: Mapped[Optional[str]] = mapped_column(String(10))
    sender_account: Mapped[Optional[str]] = mapped_column(ForeignKey("accounts.account_number"), index=True)
    receiver_account: Mapped[Optional[str]] = mapped_column(ForeignKey("accounts.account_number"), index=True)
    transaction_type: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("transaction_locations.id"), index=True)
    notes: Mapped[Optional[str]] = mapped_column(String)
    migration_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)


class ErrorRow(Base):
    __tablename__ = "error_rows"

    id: Mapped[int] = mapped_column(primary_key=True)
    context: Mapped[str] = mapped_column(Text, nullable=False)
    error_reason: Mapped[str] = mapped_column(Text, nullable=False)
    raw_data: Mapped[dict] = mapped_column(JSON, nullable=False)
    logged_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<ErrorRow(context='{self.context}', reason='{self.error_reason}')>"


class MigrationRun(Base):
    __tablename__ = "migration_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
