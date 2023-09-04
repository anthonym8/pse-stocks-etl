from pandas import DataFrame
from dotenv import load_dotenv
from os import environ
from src.utils.bigquery import query
from src.utils.misc import read_sql_file, render_template

# Prepare database credentials
load_dotenv('.env')


def test_project_id_defined():
    assert environ.get('GCP_PROJECT_ID') is not None
    
def test_credentials_path_defined():
    assert environ.get('GCP_CREDENTIALS_FILE') is not None
    
def test_gbq_location_defined():
    assert environ.get('BIGQUERY_LOCATION') is not None
    
def test_dataset_id_defined():
    assert environ.get('BIGQUERY_DATASET_ID') is not None
    
def test_query_stmt():
    stmt = 'SELECT 1 AS col;'
    assert query(stmt).to_dict('records') == [{'col':1}]
    
def test_query_sql_file():
    stmt = read_sql_file('tests/integration/sample_sql_file.sql')
    assert query(stmt).to_dict('records') == [{'col':1}]
    
def test_query_parameters():
    stmt = 'SELECT {{value}} AS {{colname}};'
    params = {'value':1, 'colname':'col'}
    assert query(stmt, parameters=params).to_dict('records') == [{'col':1}]