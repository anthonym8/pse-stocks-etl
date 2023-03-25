"""Module for extracting backfilling market data for PSE stocks"""

# Author: Rey Anthony Masilang


import pandas as pd
from src.etl.sync import PSECompanies, sync_prices


if __name__ == '__main__':
    
    pse_companies = PSECompanies()
    pse_companies.sync_db()
    
    sync_prices(pse_companies.symbols, lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
