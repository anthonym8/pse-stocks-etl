"""Miscellaneous helper functions"""

# Author: Rey Anthony Masilang


from jinja2 import Template


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


def render_template(template, parameters):
    """Substitutes SQL statement parameters with values
    
    Parameters
    ----------
    template : str
        Raw SQL statement string which contains parameter placeholders,
        e.g. {{start_date}}, {{end_date}}
        
    parameters : dict
        Dictionary of parameter-value pairs to substitute to the placeholders
        in the raw SQL statement.
    
    Returns
    -------
    stmt : str
    
    """
    
    template = Template(template)
    rendered_template = template.render(parameters)
    
    return rendered_template
