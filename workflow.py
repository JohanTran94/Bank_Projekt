from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from models import Base, ErrorRow
from datetime import datetime, timezone
import json
import math

DB_URL = "postgresql://postgres:klassrum1@localhost:5432/bank"
engine = create_engine(DB_URL)

def sanitize_nan(obj):
    if isinstance(obj, dict):
        return {k: sanitize_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_nan(i) for i in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj

def log_error(context, error_reason, raw_data):
    try:
        with engine.begin() as conn:
            for row in raw_data:
                cleaned = sanitize_nan(row)
                json_data = json.dumps(cleaned, ensure_ascii=False)
                conn.execute(
                    ErrorRow.__table__.insert().values(
                        context=context,
                        error_reason=error_reason,
                        raw_data=json_data,
                        logged_at=datetime.now(timezone.utc)
                    )
                )
    except SQLAlchemyError as db_error:
        print("Kunde inte logga till error_rows:", db_error)

@task
def import_csv(path):
    return pd.read_csv(path)

@task
def load_customers_and_accounts(df):
    try:
        customers_df = df[['Customer', 'Address', 'Phone', 'Personnummer']].drop_duplicates()
        customers_df.columns = ['name', 'address', 'phone', 'ssn']
        existing_ssn = pd.read_sql("SELECT ssn FROM customers", engine)
        filtered_customers = customers_df[~customers_df["ssn"].isin(existing_ssn["ssn"])]
        if not filtered_customers.empty:
            filtered_customers.to_sql('customers', engine, if_exists='append', index=False)

        lookup = pd.read_sql("SELECT id, ssn FROM customers", engine)
        accounts_df = df[['Personnummer', 'BankAccount']].drop_duplicates()
        accounts_df = accounts_df.merge(lookup, left_on='Personnummer', right_on='ssn', how='inner')
        accounts_df = accounts_df[['BankAccount', 'id']]
        accounts_df.columns = ['account_number', 'customer_id']
        existing_accs = pd.read_sql("SELECT account_number FROM accounts", engine)
        filtered_accounts = accounts_df[~accounts_df["account_number"].isin(existing_accs["account_number"])]
        if not filtered_accounts.empty:
            filtered_accounts.to_sql('accounts', engine, if_exists='append', index=False)
    except Exception as e:
        log_error("load_customers_and_accounts", str(e), df.to_dict(orient='records'))

@task
def load_transactions(df):
    try:
        loc_df = df[['sender_country', 'sender_municipality', 'receiver_country', 'receiver_municipality']].drop_duplicates()
        existing_locs = pd.read_sql("SELECT sender_country, sender_municipality, receiver_country, receiver_municipality FROM transaction_locations", engine)
        merged = pd.concat([loc_df, existing_locs])
        deduplicated = merged.drop_duplicates(keep=False)
        if not deduplicated.empty:
            deduplicated.to_sql('transaction_locations', engine, if_exists='append', index=False)

        loc_lookup = pd.read_sql("SELECT * FROM transaction_locations", engine)
        df = df.merge(loc_lookup, on=['sender_country', 'sender_municipality', 'receiver_country', 'receiver_municipality'], how='left')
        accounts = pd.read_sql("SELECT account_number FROM accounts", engine)
        valid_accounts = set(accounts['account_number'])
        filtered_df = df[
            df['sender_account'].isin(valid_accounts) &
            df['receiver_account'].isin(valid_accounts)
        ]
        transactions_df = filtered_df[['transaction_id', 'timestamp', 'amount', 'currency',
                                       'sender_account', 'receiver_account',
                                       'transaction_type', 'id', 'notes']]
        transactions_df.columns = ['transaction_id', 'timestamp', 'amount', 'currency',
                                   'sender_account', 'receiver_account',
                                   'transaction_type', 'location_id', 'notes']
        existing_tx = pd.read_sql("SELECT transaction_id FROM transactions", engine)
        transactions_df = transactions_df[~transactions_df["transaction_id"].isin(existing_tx["transaction_id"])]
        if not transactions_df.empty:
            transactions_df.to_sql('transactions', engine, if_exists='append', index=False)
    except Exception as e:
        log_error("load_transactions", str(e), df.to_dict(orient='records'))

@flow
def populate_normalized_database():
    customer_df = import_csv("data/sebank_customers_with_accounts.csv")
    load_customers_and_accounts(customer_df)
    transaction_df = import_csv("data/transactions.csv")
    load_transactions(transaction_df)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    populate_normalized_database()
