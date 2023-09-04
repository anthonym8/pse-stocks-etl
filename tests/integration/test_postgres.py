from pandas import DataFrame
from dotenv import load_dotenv
from os import environ
from src.utils.postgres import query
from src.utils.misc import read_sql_file, render_template

# Prepare database credentials
load_dotenv('.env')


def test_db_endpoint_defined():
    assert environ.get('POSTGRES_DB_ENDPOINT') is not None
    
def test_db_username_defined():
    assert environ.get('POSTGRES_DB_USERNAME') is not None
    
def test_db_password_defined():
    assert environ.get('POSTGRES_DB_PASSWORD') is not None
    
def test_db_port_defined():
    assert environ.get('POSTGRES_DB_PORT') is not None
    
def test_db_name_defined():
    assert environ.get('POSTGRES_DB_NAME') is not None
    
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
    
def test_query_retrieve_result_true():
    stmt = 'SELECT 1 AS col;'
    assert query(stmt, retrieve_result=True).to_dict('records') == [{'col':1}]
    
def test_query_retrieve_result_false():
    stmt = 'SELECT 1 AS col;'
    assert query(stmt, retrieve_result=False) == True