
from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from models import Base, ErrorRow
from datetime import datetime

DB_URL = "postgresql://postgres:klassrum1@localhost:5432/bank"
engine = create_engine(DB_URL)

@task
def import_csv(path):
    return pd.read_csv(path)

@task
def load_customers_and_accounts(df):
    try:
        # Rensa och förbered kunddata
        customers_df = df[['Customer', 'Address', 'Phone', 'Personnummer']].drop_duplicates()
        customers_df.columns = ['name', 'address', 'phone', 'ssn']

        # Hämta existerande personnummer för att undvika insert-fel
        existing_ssn = pd.read_sql("SELECT ssn FROM customers", engine)
        existing_set = set(existing_ssn["ssn"])
        filtered_customers = customers_df[~customers_df["ssn"].isin(existing_set)]

        if not filtered_customers.empty:
            filtered_customers.to_sql('customers', engine, if_exists='append', index=False)

        # Skapa konton och koppla till rätt kund
        lookup = pd.read_sql("SELECT id, ssn FROM customers", engine)
        accounts_df = df[['Personnummer', 'BankAccount']].drop_duplicates()
        accounts_df = accounts_df.merge(lookup, left_on='Personnummer', right_on='ssn', how='inner')
        accounts_df = accounts_df[['BankAccount', 'id']]
        accounts_df.columns = ['account_number', 'customer_id']

        # Hämta existerande konton
        existing_accs = pd.read_sql("SELECT account_number FROM accounts", engine)
        existing_acc_set = set(existing_accs["account_number"])
        filtered_accounts = accounts_df[~accounts_df["account_number"].isin(existing_acc_set)]

        if not filtered_accounts.empty:
            filtered_accounts.to_sql('accounts', engine, if_exists='append', index=False)
    except Exception as e:
        log_error("load_customers_and_accounts", str(e), df.to_dict(orient='records'))

@task
def load_transactions(df):
    try:
        # Förbered platsdata och undvik duplicat
        loc_df = df[['sender_country', 'sender_municipality', 'receiver_country', 'receiver_municipality']].drop_duplicates()
        existing_locs = pd.read_sql("SELECT sender_country, sender_municipality, receiver_country, receiver_municipality FROM transaction_locations", engine)
        merged = pd.concat([loc_df, existing_locs])
        deduplicated = merged.drop_duplicates(keep=False)

        if not deduplicated.empty:
            deduplicated.to_sql('transaction_locations', engine, if_exists='append', index=False)

        # Slå upp location_id
        loc_lookup = pd.read_sql("SELECT * FROM transaction_locations", engine)
        df = df.merge(loc_lookup, on=['sender_country', 'sender_municipality', 'receiver_country', 'receiver_municipality'], how='left')

        # Validera konton
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

        # Undvik duplicerade transaktioner
        existing_tx = pd.read_sql("SELECT transaction_id FROM transactions", engine)
        existing_tx_set = set(existing_tx["transaction_id"])
        transactions_df = transactions_df[~transactions_df["transaction_id"].isin(existing_tx_set)]

        if not transactions_df.empty:
            transactions_df.to_sql('transactions', engine, if_exists='append', index=False)
    except Exception as e:
        log_error("load_transactions", str(e), df.to_dict(orient='records'))

def log_error(context, error_reason, raw_data):
    try:
        with engine.begin() as conn:
            for row in raw_data:
                conn.execute(
                    ErrorRow.__table__.insert().values(
                        context=context,
                        error_reason=error_reason,
                        raw_data=row,
                        logged_at=datetime.utcnow()
                    )
                )
    except SQLAlchemyError as db_error:
        print("Kunde inte logga till error_rows:", db_error)

@flow
def populate_normalized_database():
    customer_df = import_csv("data/sebank_customers_with_accounts.csv")
    load_customers_and_accounts(customer_df)

    transaction_df = import_csv("data/transactions.csv")
    load_transactions(transaction_df)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    populate_normalized_database()
