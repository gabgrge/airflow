import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# Get the absolute path of the project's root directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Define the DAG schedule and default arguments
dag = DAG(
    'monthly_statistics',
    description='ETL pipeline for generating monthly statistics by restaurants',
    schedule_interval='@monthly',
    start_date=datetime(2023, 7, 1),
    catchup=False
)


# Function to read data from bookings.csv and load it into PostgreSQL
def extract_and_load_data():
    # Construct the absolute file path to bookings.csv
    csv_file_path = os.path.join(project_root, 'bookings.csv')

    # Read data from bookings.csv
    df = pd.read_csv(csv_file_path)

    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine('postgresql://airflow:admin@localhost/airflow')

    # Load data into a staging bookings table
    df.to_sql('bookings_table', engine, if_exists='replace', index=False)


# Function to preprocess data and load it into PostgreSQL
def preprocess_and_load_data():
    import re
    from forex_python.converter import CurrencyRates

    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine('postgresql://airflow:admin@localhost/airflow')

    # Execute a query to select all rows from the bookings_table and read the results into a DataFrame
    df = pd.read_sql_query("SELECT * FROM bookings_table;", engine)

    # Convert the date column to datetime data type while handling invalid formats
    df['date'] = df['date'].str.replace('/', '-')
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y', errors='coerce')

    # Define the currency symbols mapping to currency codes
    currency_symbols = {
        '£': 'GBP',
        '€': 'EUR',
    }

    # Fetch the latest exchange rates using forex-python
    c = CurrencyRates()
    base_currency = 'EUR'
    exchange_rates = c.get_rates(base_currency)

    # Function to convert amount to Euro currency
    def convert_to_euro(amount, currency):
        currency_code = currency_symbols.get(currency, base_currency)
        return amount / exchange_rates[currency_code] if currency_code in exchange_rates else amount

    # Preprocess the amount column to extract and put the currency symbol in a new currency column
    df['currency'] = df['amount'].apply(lambda x: re.sub(r'[\s\d.,]', '', str(x)))

    # Preprocess the amount column to remove currency symbols, white spaces and to convert amounts to float
    df['amount'] = df['amount'].replace(r'[^\d.,]+', '', regex=True).replace(',', '.', regex=True).astype(float)

    # Convert each amount to Euro currency using the convert_to_euro function
    df['amount_euro'] = df.apply(lambda row: convert_to_euro(row['amount'], row['currency']), axis=1)

    # Load data into a staging preprocessed table
    df.to_sql('preprocessed_table', engine, if_exists='replace', index=False)


# Function to transform data, calculate monthly statistics and load results into PostgreSQL
def transform_and_load_data():
    # Create a SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine('postgresql://airflow:admin@localhost/airflow')

    with engine.connect() as conn:
        # Execute the table creation query and perform data transformation and calculate monthly statistics
        conn.execute("""
            CREATE TABLE IF NOT EXISTS monthly_restaurants_report (
                restaurant_id TEXT,
                restaurant_name TEXT,
                country TEXT,
                month TEXT,
                number_of_bookings BIGINT,
                number_of_guests BIGINT,
                amount FLOAT,
                PRIMARY KEY (restaurant_id, month)
            )
        """)

        # Use INSERT ... ON CONFLICT to handle duplicate key violations
        conn.execute("""
            INSERT INTO monthly_restaurants_report (restaurant_id, restaurant_name, country, month, number_of_bookings, number_of_guests, amount)
            SELECT
                restaurant_id,
                restaurant_name,
                country,
                DATE_TRUNC('month', date)::date AS month,
                COUNT(*) AS number_of_bookings,
                SUM(guests) AS number_of_guests,
                SUM(amount_euro) AS amount
            FROM preprocessed_table
            GROUP BY restaurant_id, restaurant_name, country, month
            ON CONFLICT (restaurant_id, month)
            DO UPDATE SET
                number_of_bookings = EXCLUDED.number_of_bookings,
                number_of_guests = EXCLUDED.number_of_guests,
                amount = EXCLUDED.amount
        """)


# Define the DAG tasks
extract_and_load_task = PythonOperator(
    task_id='extract_and_load_data',
    python_callable=extract_and_load_data,
    dag=dag
)

preprocess_and_load_task = PythonOperator(
    task_id='preprocess_and_load_data',
    python_callable=preprocess_and_load_data,
    dag=dag
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load_data',
    python_callable=transform_and_load_data,
    dag=dag
)

# Set the task dependencies
extract_and_load_task >> preprocess_and_load_task >> transform_and_load_task
