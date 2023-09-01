"""Script for creating the database tables."""

# Author: Rey Anthony Masilang


from src.utils.postgres import query


def create_tables() -> None:
    """Creates the relevant tables in the Postgres database."""
    
    query(sql_file='src/db/postgres/trigger_set_timestamp.sql', retrieve_result=False)
    print('Created function: trigger_set_timestamp')
    
    query(sql_file='src/db/postgres/company.sql', retrieve_result=False)
    print('Created table: pse.company')
    
    query(sql_file='src/db/postgres/daily_stock_price.sql', retrieve_result=False)
    print('Created table: pse.daily_stock_price')


if __name__ == '__main__':
    create_tables()