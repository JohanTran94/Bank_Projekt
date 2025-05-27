from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine

@task
def import_csv(csv_path):
    dataframe = pd.read_csv(csv_path)
    return dataframe

@task
def write_to_postgres(dataframe, table_name, db_url):
    engine = create_engine(db_url)
    dataframe.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

@flow
def load_multiple_csvs():
    csv_table_list = [
        ("C:/Users/johan/Desktop/Johan/DM-TUC-24/Bank_test/data/transactions.csv", "transactions"),
        ("C:/Users/johan/Desktop/Johan/DM-TUC-24/Bank_test/data/sebank_customers_with_accounts.csv", "sebank_customers_with_accounts")
    ]

    db_url = "postgresql://postgres:Jason.Chen241194@localhost:5544/bank_db"

    for csv_path, table_name in csv_table_list:
        df = import_csv(csv_path)
        write_to_postgres(df, table_name, db_url)

if __name__ == "__main__":
    load_multiple_csvs()
