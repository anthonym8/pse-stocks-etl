"""Module for syncing market data for PSE stocks"""

# Author: Rey Anthony Masilang


import pandas as pd
from datetime import datetime, timedelta
from src.utils.pse_edge import get_listed_companies, get_stock_data
from src.utils.postgres import query
from typing import List
    
    
class PSECompanies:
    """Dataset class for PSE-listed companies."""
    
    def __init__(self):
        self._refresh_metadata()
        
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        symbols_df = query("SELECT symbol FROM pse.company ORDER BY symbol;")
        self.symbols = symbols_df.symbol.tolist()  # List of symbols
        
    def _delete_all_records(self) -> None:
        """Deletes all database records."""
        query("DELETE FROM pse.company;", retrieve_result=False)
        self._refresh_metadata()
    
    def fetch_db_records(self) -> pd.DataFrame:
        """Fetches the current database records."""
        data = query("SELECT * FROM pse.company ORDER BY symbol;")
        return data
    
    def sync_db(self, batch_size: int = 100) -> None:
        """Syncs database table to PSE Edge data.
        
        Parameters
        ----------
        batch_size : int, default 100
            The number of records to insert at a time.

        Returns
        -------
        None

        """
        
        # Extract source data
        source_data = get_listed_companies()
        
        # Insert new data to DB
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
        n_companies = source_data.shape[0]
        for idx in range(n_companies):
            symbol, company_name, sector, subsector, listing_date, extracted_at = \
                source_data.iloc[idx].loc[['symbol', 'company_name', 'sector', 'subsector', 'listing_date', 'extracted_at']]

            row_str = f"('{symbol}','{company_name}','{sector}','{subsector}','{listing_date}','{extracted_at}')"
            rows_to_insert.append(row_str)

            if (len(rows_to_insert) == batch_size) or (idx+1 == n_companies):
                print(f'  {idx+1} out of {n_companies}')
                stmt = INSERT_STMT_TEMPLATE.format(tuples=',\n           '.join(rows_to_insert))
                query(stmt=stmt, retrieve_result=False)
                rows_to_insert = []
                
        self._refresh_metadata()
                
    
class DailyStockPriceDataset:
    """Dataset class for the daily stock price table.
    
    Parameters
    ----------
    symbols : str
        A list of ticker symbols for PSE-listed companies.
    
    """
    
    def __init__(self, symbols : List[str]):
        self.symbols = symbols
        self.n_symbols = len(symbols)
        self._refresh_metadata()
    
    def _get_latest_dates(self) -> dict:
        """Fetches the current database state."""
        stmt = f"""
            SELECT symbol, max(date) AS latest_date
            FROM pse.daily_stock_price
            GROUP BY symbol;
        """

        df = query(stmt=stmt)
        latest_dates = df.set_index('symbol').to_dict(orient='dict')['latest_date']
        
        return latest_dates
    
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        latest_dates = self._get_latest_dates()
        self.latest_dates = {s:latest_dates.get(s, None) for s in self.symbols}
        
    def _delete_all_records(self) -> None:
        """Deletes all database records."""
        query("DELETE FROM pse.daily_stock_price;", retrieve_result=False)
        self._refresh_metadata()
        
    def _insert_price_data_to_db(self, df: pd.DataFrame, batch_size: int = 3000) -> None:
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
                
        
    def sync_db(self, lookback_days : int = 0, freshness_days : int = 1) -> None:
        """Updates price data for all companies.

        Parameters
        ----------
        lookback_days : int, default 0
            The number of days in the past to re-extract price data for.

        freshness_days : int, default 1
            The acceptable data delay in number of days. By default, this is set to 1
            which means data for yesterday is the minimum acceptable value to consider
            that the price data is already up-to-date.
            
        Returns
        -------
        None

        """
        
        for idx in range(self.n_symbols):
            symbol = self.symbols[idx]
            print(f'Updating price data:  {symbol:6s}  ({idx+1} out of {self.n_symbols})')

            target_latest_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            latest_date = self.latest_dates.get(symbol, None)

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
                self._insert_price_data_to_db(price_df)
                
        self._refresh_metadata()

        
def sync() -> None:
    """Executes an incremental sync job."""
    
    pse_companies = PSECompanies()
    pse_companies.sync_db()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_db()
    
    
def backfill() -> None:
    """Executes a complete backfill job."""
    
    pse_companies = PSECompanies()
    pse_companies.sync_db()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_db(lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
    

if __name__ == '__main__':
    
    sync()