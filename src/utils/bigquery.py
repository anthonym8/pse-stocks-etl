"""Google Cloud Storage helper functions"""

# Author: Rey Anthony Masilang


import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from os import environ
from google.cloud import storage
from src.utils.misc import read_sql_file, render_template


__all__ = [
    'execute',
]


# Prepare database credentials
load_dotenv('.env')

GCP_PROJECT_ID = environ.get('GCP_PROJECT_ID')
GCP_CREDENTIALS_FILE = environ.get('GCP_CREDENTIALS_FILE')
BIGQUERY_LOCATION = environ.get('BIGQUERY_LOCATION')


def execute(sql_statement=None, sql_file=None, parameters=None, return_results=False,
            project_id=GCP_PROJECT_ID, credentials_file_path=GCP_CREDENTIALS_FILE, 
            location=BIGQUERY_LOCATION):
    """Executes a SQL command on BigQuery.
    
    Parameters
    ----------
    sql_statement : str, default None
        SQL statement string. This statement is used instead if sql_file is left
        blank.
        
    sql_file : str, default None
        Path to .sql text file. This file is parsed and used as statement to
        extract data from the source database.
        
    parameters : dict, default None
        Dictionary of parameter-value pairs to substitute to the placeholders
        in the raw SQL statement.
        
    return_results : bool, default False
        Returns the query results if set to True.
        
    project : str
        The name of the GCP project where the bucket exists.
        
    credentials_file_path : str
        The local path to the JSON credentials file for the GCP project.
        
    location : str
        The GCP location of the BigQuery dataset.

    Returns
    -------
    result : pandas.DataFrame
        A DataFrame of the query results if return_results is set to True.
        Otherwise, the function returns None.
        
    """
        
    # Read SQL statement from file
    if sql_file is not None:
        sql_statement = read_sql_file(sql_file)
        
    # Template query as needed
    if parameters is not None:
        sql_statement = render_template(sql_statement, parameters)
    
    # Initialize the BigQuery client
    client = bigquery.Client.from_service_account_json(credentials_file_path, project=project_id, location=location)

    # Execute the DML statement
    query_job = client.query(sql_statement)

    # Wait for the query job to complete
    result = query_job.result()
    
    if return_results:
        # Convert the results to a Pandas DataFrame
        df = pd.DataFrame(data=[list(row.values()) for row in result], columns=[f.name for f in result.schema])
        return df
    
    else:
        return None
    
    
def query(sql_statement=None, sql_file=None, parameters=None, **kwargs):
    """Executes a SQL query and returns the results.
    
    Parameters
    ----------
    sql_statement : str, default None
        SQL statement string. This statement is used instead if sql_file is left
        blank.
        
    sql_file : str, default None
        Path to .sql text file. This file is parsed and used as statement to
        extract data from the source database.
        
    parameters : dict, default None
        Dictionary of parameter-value pairs to substitute to the placeholders
        in the raw SQL statement.
        
    **kwargs:
        Additional keyword arguments to pass to the execute function.

    Returns
    -------
    result : pandas.DataFrame
        A DataFrame of the query results.
    
    """
    
    result = execute(sql_statement=sql_statement,
                     sql_file=sql_file,
                     parameters=parameters, 
                     return_results=True, 
                     **kwargs)
    
    return result
