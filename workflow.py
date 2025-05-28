from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine
from models import Base

DB_URL = "postgresql://postgres:klassrum1@localhost:5432/bank"
engine = create_engine(DB_URL)

@task
def import_csv(path):
    return pd.read_csv(path)

@task
def load_customers_and_accounts(df):
    engine = create_engine(DB_URL)
    customers_df = df[['Customer', 'Address', 'Phone', 'Personnummer']].drop_duplicates()
    customers_df.columns = ['name', 'address', 'phone', 'ssn']
    customers_df.to_sql('customers', engine, if_exists='append', index=False)

    lookup = pd.read_sql("SELECT id, ssn FROM customers", engine)
    accounts_df = df[['Personnummer', 'BankAccount']].drop_duplicates()
    accounts_df = accounts_df.merge(lookup, left_on='Personnummer', right_on='ssn', how='inner')
    accounts_df = accounts_df[['BankAccount', 'id']]
    accounts_df.columns = ['account_number', 'customer_id']
    accounts_df.to_sql('accounts', engine, if_exists='append', index=False)

@task
def load_transactions(df):
    engine = create_engine(DB_URL)

    # Transaction locations
    loc_df = df[['sender_country', 'sender_municipality', 'receiver_country', 'receiver_municipality']].drop_duplicates()
    loc_df.to_sql('transaction_locations', engine, if_exists='append', index=False)

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
    transactions_df.to_sql('transactions', engine, if_exists='append', index=False)

@flow
def populate_normalized_database():
    customer_df = import_csv("data/sebank_customers_with_accounts.csv")
    load_customers_and_accounts(customer_df)

    transaction_df = import_csv("data/transactions.csv")
    load_transactions(transaction_df)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    populate_normalized_database()
