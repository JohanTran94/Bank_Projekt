from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
db_url = os.getenv("DB_URL")

@task
def import_csv(csv_path):
    dataframe = pd.read_csv(csv_path)
    return dataframe

@task
def write_to_postgres(dataframe, table_name):
    engine = create_engine(db_url)
    dataframe.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

@flow
def load_multiple_csv():
    csv_table_list = [
        ("/Users/jonaslarsson/PycharmProjects/Bank_Projekt/data/transactions.csv", "transactions"),
        ("/Users/jonaslarsson/PycharmProjects/Bank_Projekt/data/sebank_customers_with_accounts.csv", "sebank_customers_with_accounts")
    ]

    for csv_path, table_name in csv_table_list:
        df = import_csv(csv_path)
        write_to_postgres(df, table_name)

if __name__ == "__main__":
    load_multiple_csv()
