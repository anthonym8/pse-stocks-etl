"""PostgreSQL helper functions"""

# Author: Rey Anthony Masilang


from pandas import read_sql_query
from pandas.io import sql
from psycopg2 import connect as db_connect
from dotenv import load_dotenv
from os import environ
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool


__all__ = [
    'read_sql_file',
    'template_query',
    'query'
]


# Prepare database credentials
load_dotenv('.env')

creds = {
    "db_endpoint": environ.get('DATABASE_ENDPOINT'),
    "db_username": environ.get('DATABASE_USERNAME'),
    "db_password": environ.get('DATABASE_PASSWORD'),
    "db_port": environ.get('DATABASE_PORT'),
    "db_name": environ.get('DATABASE_NAME')
}


def read_sql_file(sql_file):
    """Read sql statement from sql file into a string.
    
    Parameters
    ----------
    sql_file : str
        Path to .sql text file
        
    Returns
    -------
    sql_stmt : str
        SQL statement string
    
    """
    
    with open (sql_file, "r") as myfile:
        sql_lines = myfile.readlines()
        sql_stmt = ''.join(sql_lines)
    return sql_stmt


def template_query(stmt, parameters):
    """Substitutes SQL statement parameters with values
    
    Parameters
    ----------
    stmt : str
        Raw SQL statement string which contains parameter placeholders,
        e.g. {{start_date}}, {{end_date}}
        
    parameters : dict
        Dictionary of parameter-value pairs to substitute to the placeholders
        in the raw SQL statement.
    
    Returns
    -------
    stmt : str
    
    """

    for key, value in parameters.items():
        stmt = stmt.replace('{{{{{}}}}}'.format(key), str(value))
    return stmt


def query(stmt=None, sql_file=None, parameters=None,
          retrieve_result=True):
    """Executes a SQL command.
    
    Parameters
    ----------
    stmt : str, default None
        SQL statement string. This statement is used instead if sql_file is left
        blank.
        
    sql_file : str, default None
        Path to .sql text file. This file is parsed and used as statement to
        extract data from the source database.
        
    parameters : dict
        Dictionary of parameter-value pairs to substitute to the placeholders
        in the raw SQL statement.
                
    retrieve_results : bool, default True
        Flag to retrieve query results or not.
                        
    Returns
    -------
    result : pandas.DataFrame
        A DataFrame of the query results
    
    """
        
    # Read SQL statement from file
    if sql_file is not None:
        stmt = read_sql_file(sql_file)
        
    # Template query as needed
    if parameters is not None:
        stmt = template_query(stmt, parameters)

    # Create database connection
    conn = db_connect(
        host=creds['db_endpoint'],
        port=creds['db_port'],
        user=creds['db_username'],
        password=creds['db_password'],
        dbname=creds['db_name']
    )
    
    # Use SQLAlchemy to remove pandas warning
    engine = create_engine('postgresql+psycopg2://', poolclass=StaticPool, creator= lambda: conn)

    if retrieve_result:
        result = read_sql_query(stmt, con=engine)
    else:
        sql.execute(stmt, con=engine)
        result = True
        
    return result
