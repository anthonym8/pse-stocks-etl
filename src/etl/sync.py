"""Module for syncing market data for PSE stocks"""

# Author: Rey Anthony Masilang


import pandas as pd
from datetime import datetime, timedelta
from src.utils.pse_edge import get_listed_companies, get_stock_data
from src.utils.postgres import query


class DatabasePriceTableFreshness:
    """Contains the current price data freshness for all ticker symbols in the database"""
    
    def __init__(self):
        self.refresh()
        
    def refresh(self):
        """Fetches the current database state."""
        stmt = f"""
            SELECT symbol, max(date) AS latest_date
            FROM pse.daily_stock_price
            GROUP BY symbol;
        """

        df = query(stmt=stmt)
        self.latest_dates = df.set_index('symbol').to_dict(orient='dict')['latest_date']
        
    def get_latest_date(self, symbol : str) -> datetime:
        """Returns the latest price data point for a specified ticker symbol."""
        return self.latest_dates.get(symbol, None)
    

def insert_companies_to_db(df: pd.DataFrame, batch_size: int = 100) -> None:
    """Inserts company records to database.
    
    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame of companies which are part of the Philippine Stock Exchange.
      
    batch_size : int, default 100
        The number of records to insert at a time.
      
    Returns
    -------
    None
    
    """
    
    n_symbols = df.shape[0]
    
    print('Inserting rows to database.')

    INSERT_STMT_TEMPLATE = r"""
        INSERT INTO pse.company (symbol, company_name, sector, subsector, listing_date, extracted_at)
        VALUES {tuples}
        ON CONFLICT (symbol)
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            company_name = EXCLUDED.company_name,
            sector = EXCLUDED.sector,
            subsector = EXCLUDED.subsector,
            listing_date = EXCLUDED.listing_date,
            extracted_at = EXCLUDED.extracted_at;

        COMMIT;
    """

    rows_to_insert = []
    for idx in range(n_symbols):
        symbol, company_name, sector, subsector, listing_date, extracted_at = \
            df.iloc[idx].loc[['symbol', 'company_name', 'sector', 'subsector', 'listing_date', 'extracted_at']]

        row_str = f"('{symbol}','{company_name}','{sector}','{subsector}','{listing_date}','{extracted_at}')"
        rows_to_insert.append(row_str)

        if (len(rows_to_insert) == batch_size) or (idx+1 == n_symbols):
            print(f'  {idx+1} out of {n_symbols}')
            stmt = INSERT_STMT_TEMPLATE.format(tuples=',\n           '.join(rows_to_insert))
            query(stmt=stmt, retrieve_result=False)
            rows_to_insert = []


def sync_companies() -> pd.DataFrame:
    """Updates the companies dataset."""
            
    # Extract complete list of companies
    companies_df = get_listed_companies()

    # Load to database
    insert_companies_to_db(companies_df)
    
    return companies_df
    

def get_new_price_data(symbol: str, lookback_days : int = 0, freshness_days : int = 1) -> pd.DataFrame:
    """
    Extracts missing price data for a specific company starting from the
    latest date in the database up to the current date.
    
    Parameters
    ----------
    symbol : str
        The ticker symbol of the company.
    
    lookback_days : int, default 0
        The number of days in the past to re-extract price data for.
        
    freshness_days : int, default 1
        The acceptable data delay in number of days. By default, this is set to 1
        which means data for yesterday is the minimum acceptable value to consider
        that the price data is already up-to-date.
    
    Returns
    -------
    price_df : pandas.DataFrame
        A dataframe of dates and prices for the specified company.
    
    """
    
    target_latest_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
    latest_date = get_latest_date_from_db(symbol)
    
    # Skip data extraction if conditions are satisfied
    if lookback_days==0 and latest_date==target_latest_date:
        # Return empty data frame.
        price_df = pd.DataFrame(columns=['symbol','date','open','high','low','close','extracted_at'])
        print('  Already up-to-date. Skipping.')
        
    else:
        if latest_date is not None: 
            start_date = (latest_date + timedelta(days=1-lookback_days)).strftime('%Y-%m-%d')
        else:
            start_date = None
            
        price_df = get_stock_data(symbol, start_date=start_date)
        print(f'  Fetched {price_df.shape[0]} records.')
    
    return price_df


def insert_price_data_to_db(df: pd.DataFrame, batch_size: int = 3000) -> None:
    """Inserts price data records to the database.
    
    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame of price data to insert to the database.
      
    batch_size : int, default 100
        The number of records to insert at a time.
      
    Returns
    -------
    None
    
    """
    
    INSERT_STMT_TEMPLATE = r"""
        INSERT INTO pse.daily_stock_price (symbol, date, open, high, low, close, extracted_at)
        VALUES {tuples}
        ON CONFLICT (symbol,date)
        DO UPDATE SET
            "symbol" = EXCLUDED.symbol,
            "date" = EXCLUDED.date,
            "open" = EXCLUDED.open,
            "high" = EXCLUDED.high,
            "low" = EXCLUDED.low,
            "close" = EXCLUDED.close,
            "extracted_at" = EXCLUDED.extracted_at;

        COMMIT;
    """
    
    n_rows = df.shape[0]
    rows_to_insert = []
    for idy in range(n_rows):
        p_symbol, p_date, p_open, p_high, p_low, p_close, p_extracted_at = \
            df.iloc[idy].loc[['symbol','date','open','high','low','close','extracted_at']]

        row_str = f"('{p_symbol}', '{p_date}', {p_open}, {p_high}, {p_low}, {p_close}, '{p_extracted_at}')"
        rows_to_insert.append(row_str)

        if (len(rows_to_insert) == batch_size) or (idy+1 == n_rows):
            print(f'  Inserted {len(rows_to_insert)} records.')
            stmt = INSERT_STMT_TEMPLATE.format(tuples = ',\n           '.join(rows_to_insert))
            query(stmt=stmt, retrieve_result=False)
            rows_to_insert = []
            
            
def sync_prices(companies_df : pd.DataFrame, lookback_days : int = 0, freshness_days : int = 1) -> None:
    """Updates price data for all companies.
    
    Parameters
    ----------
    companies_df : pandas.DataFrame
        A DataFrame of PSE companies with their ticker symbols.
        
    lookback_days : int, default 0
        The number of days in the past to re-extract price data for.
        
    freshness_days : int, default 1
        The acceptable data delay in number of days. By default, this is set to 1
        which means data for yesterday is the minimum acceptable value to consider
        that the price data is already up-to-date.
    
    """
    
    db_state = DatabasePriceTableFreshness()

    n_symbols = companies_df.shape[0]
    for idx in range(n_symbols):
        symbol = companies_df.iloc[idx].loc['symbol']
        print(f'Updating price data:  {symbol:6s}  ({idx+1} out of {n_symbols})')
        
        target_latest_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
        latest_date = db_state.get_latest_date(symbol)

        # Skip data extraction if conditions are satisfied
        if lookback_days==0 and latest_date==target_latest_date:
            # Return empty data frame.
            price_df = pd.DataFrame(columns=['symbol','date','open','high','low','close','extracted_at'])
            print('  Already up-to-date. Skipping.')

        # Extract new price data
        else:
            if latest_date is not None: 
                start_date = (latest_date + timedelta(days=1-lookback_days)).strftime('%Y-%m-%d')
            else:
                start_date = None

            price_df = get_stock_data(symbol, start_date=start_date)
            print(f'  Fetched {price_df.shape[0]} records.')

        # Insert to database
        if price_df.shape[0] == 0:
            print('  No new records. Skipping.')
        else:
            insert_price_data_to_db(price_df)


if __name__ == '__main__':
    
    companies_df = sync_companies()
    _ = sync_prices(companies_df)