"""PostgreSQL helper functions"""

# Author: Rey Anthony Masilang


from pandas import read_sql_query
from pandas.io import sql
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

DB_HOST = environ.get('DATABASE_ENDPOINT')
DB_PORT = environ.get('DATABASE_PORT')
DB_NAME = environ.get('DATABASE_NAME')
DB_USER = environ.get('DATABASE_USERNAME')
DB_PASSWORD = environ.get('DATABASE_PASSWORD')


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


def query(stmt=None, sql_file=None, parameters=None, retrieve_result=True):
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
                
    retrieve_result : bool, default True
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
        
    # Establish a connection to the database using sqlalchemy
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    conn = engine.connect()
    
    # Execute the SQL command
    if retrieve_result:
        result = read_sql_query(stmt, conn)
    else:
        conn.execute(stmt)
        result = True
        
    # Close the cursor and database connection
    conn.close()
        
    return result
