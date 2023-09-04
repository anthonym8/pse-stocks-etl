"""Script for creating the database tables."""

# Author: Rey Anthony Masilang


from dotenv import load_dotenv
from os import environ
from src.utils.bigquery import execute


# Prepare database credentials
load_dotenv('.env')

GCP_PROJECT_ID = environ.get('GCP_PROJECT_ID')
BIGQUERY_DATASET_ID = environ.get('BIGQUERY_DATASET_ID')


def create_tables() -> None:
    """Creates the relevant tables in the Postgres database."""
    
    parameters = {'project_id':GCP_PROJECT_ID, 'dataset_id':BIGQUERY_DATASET_ID}
    
    execute(sql_file='src/db/bigquery/company.sql', parameters=parameters)
    print(f'Created table: {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.company')
    
    execute(sql_file='src/db/bigquery/daily_stock_price.sql', parameters=parameters)
    print(f'Created table: {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.daily_stock_price')


if __name__ == '__main__':
    create_tables()