"""Module for extracting backfilling market data for PSE stocks"""

# Author: Rey Anthony Masilang


from src.etl.sync import sync_companies, sync_prices


if __name__ == '__main__':
    
    companies_df = sync_companies()
    _ = sync_prices(companies_df, lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
