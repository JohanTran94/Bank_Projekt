# ETL Workflow with Automated Data Quality Routines

This project provides a modular ETL pipeline for ingesting, transforming, and validating data from external sources into a PostgreSQL database. It supports both manual and automated workflows and includes integrated data quality checks, batch processing, and workflow orchestration.

---

## Tech Stack

- Python 3.11+
- PostgreSQL
- Dask – Efficient batch processing
- Alembic – Database schema versioning
- Prefect – Workflow orchestration
- Great Expectations – Data validation
- Pandas / NumPy – Data transformations
- FastAPI – (optional, for future API integration)

---

## Setup Instructions

1. Clone the repository:

   ```bash
   git clone https://github.com/JohanTran94/Bank_Projekt.git
   cd Bank_Projekt
   ```

2. Create a virtual environment and install dependencies:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. Configure your database connection:
   - Add your database connection string to `.env`
   - Update the connection string in:
     - `alembic.ini` (line 87)
     - `db.py`, function `create_conn()` (line 16)

---

## Manual ETL Flow

1. Ensure your PostgreSQL database is running.

2. Run the following commands in the terminal:

   ```bash
   python init_db.py       # Initializes database schema
   python load_data.py     # Loads and transforms data using Dask
   ```

3. Features:
   - Batch inserts with Dask
   - Logs errors to:
     - `error_rows` table in the database
     - `.csv` files in the `_logs/` directory
   - Automatically creates Alembic migrations if schema changes
   - Each run is logged in:
     - The `migration_runs` table
     - The `alembic/versions/` directory

4. To drop all tables, run:

   ```bash
   python drop_all_tables.py
   ```

---

## Automated ETL Flow (with Prefect)

1. Ensure your PostgreSQL database is running.

2. Run the workflow:

   ```bash
   python workflow.py
   ```

This runs the full ETL flow described above, wrapped in a Prefect workflow for improved automation, logging, and observability.

---

## Manual Data Validation (Jupyter + Great Expectations)

Use the included Jupyter notebooks for manual validation or exploration of raw `.csv` files:

1. Validate customers with accounts:
   - Open `validate-customers_with_accounts.ipynb`
   - Set the file path on line 8
   - Run the notebook

2. Validate transactions:
   - Open `validate-transactions.ipynb`
   - Set the file path on line 6
   - Run the notebook

### Outputs:

- `validation_results_transactions.json`
- `transactions_cleaned.csv`

Logged in the `reports/` directory.

---

## License

MIT License

---

## Authors

- Johan Tran  
- Thomas Rosén  
- Max Nilsson  
- Jonas Larsson  

With great help from Benjamin Berglund
# Bank
