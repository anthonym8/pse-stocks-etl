from src.utils.misc import read_sql_file, render_template


def test_read_sql_file():
    sql_stmt = 'SELECT 1 AS col;'
    assert sql_stmt == read_sql_file('tests/integration/sample_sql_file.sql')

def test_render_template():
    raw_stmt = """
        SELECT
              '{{string_value}}' AS {{string_column}}
            , {{int_value}} AS {{int_column}}
            
        FROM {{schema}}.{{table}};
    """
    
    templated_stmt = """
        SELECT
              'sample_string' AS my_str_col
            , 10 AS my_int_col
            
        FROM my_schema.my_table;
    """
    
    params = {
        'schema':'my_schema',
        'table':'my_table',
        'string_value':'sample_string',
        'string_column':'my_str_col',
        'int_value':10,
        'int_column':'my_int_col'
    }
    
    assert templated_stmt == render_template(raw_stmt, params)