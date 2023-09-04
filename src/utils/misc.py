"""Miscellaneous helper functions"""

# Author: Rey Anthony Masilang


import os
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


def prepare_directory(file_path):
    """Creates the directory given a file path if it does not exist yet.
    
    Parameters
    ----------
    file_path : str
        The full path to a file or directory. The function extracts the 
        directory path from this and creates it as needed.
        
    Returns
    -------
    None
    
    """
    
    # Extract the directory path from the file_path
    directory = os.path.dirname(file_path)

    # Check if the directory exists; if not, create it
    if not os.path.exists(directory):
        os.makedirs(directory)

        
def delete_files(x, verbose=True):
    """Deletes a file or multiple files from disk.

    Parameters
    ----------
    x : str or list of str
        The path(s) to the file(s) to be deleted.
        
    verbose : bool, default True
        Prints out status messages to console if set to True.

    Returns
    -------
    None
    
    """
    
    def delete_file(file_path):
        try:
            os.remove(file_path)
            if verbose: print(f"File '{file_path}' has been deleted.")
        except FileNotFoundError:
            if verbose: print(f"File '{file_path}' not found, no action taken.")
        except Exception as e:
            if verbose: print(f"An error occurred while trying to delete '{file_path}': {str(e)}")
            
    if type(x) == str:
        delete_file(x)
        
    elif type(x) == list:
        for f in x:
            delete_file(f)