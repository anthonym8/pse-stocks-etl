"""Module for syncing PSE market data to a Postgres database"""

# Author: Rey Anthony Masilang


import pandas as pd
from datetime import datetime, timedelta
from src.utils.pse_edge import get_listed_companies, get_stock_data
from src.utils.postgres import query
from src.utils.multithreading import parallel_execute
from typing import List
    
    
class PSECompaniesDataset:
    """Dataset class for PSE-listed companies."""
    
    def __init__(self):
        self.source_data = None
        self.db_data = None
        self.symbols = None
        self._fetch_source_data()
        self._fetch_db_data()
        
    def _delete_all_records(self) -> None:
        """Deletes all database records."""
        query("DELETE FROM pse.company;", retrieve_result=False)
        self._fetch_db_data()
        
    def _fetch_source_data(self) -> pd.DataFrame:
        """Fetches data from source."""
        self.source_data = get_listed_companies()
        self.symbols = self.source_data.symbol.tolist()
        return self.source_data
    
    def _fetch_db_data(self) -> pd.DataFrame:
        """Fetches the current database records."""
        self.db_data = query("SELECT * FROM pse.company ORDER BY symbol;")
        return self.db_data
            
    def sync_table(self, batch_size: int = 100) -> None:
        """Syncs database table to PSE Edge data.
        
        Parameters
        ----------
        batch_size : int, default 100
            The number of records to insert at a time.

        Returns
        -------
        None

        """
                
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
        n_companies = self.source_data.shape[0]
        for idx in range(n_companies):
            symbol, company_name, sector, subsector, listing_date, extracted_at = \
                self.source_data.iloc[idx].loc[['symbol', 'company_name', 'sector', 'subsector', 'listing_date', 'extracted_at']]

            row_str = f"('{symbol}','{company_name}','{sector}','{subsector}','{listing_date}','{extracted_at}')"
            rows_to_insert.append(row_str)

            if (len(rows_to_insert) == batch_size) or (idx+1 == n_companies):
                print(f'  {idx+1} out of {n_companies}')
                stmt = INSERT_STMT_TEMPLATE.format(tuples=',\n           '.join(rows_to_insert))
                query(stmt=stmt, retrieve_result=False)
                rows_to_insert = []
                
        self._fetch_db_data()
                
    
class DailyStockPriceDataset:
    """Dataset class for the daily stock price table.
    
    Parameters
    ----------
    symbols : str
        A list of ticker symbols for PSE-listed companies.
    
    """
    
    def __init__(self, symbols : List[str]):
        self.symbols = symbols
        self.db_latest_dates = {}
        self._get_db_latest_dates()
    
    def _get_db_latest_dates(self) -> dict:
        """Fetches the current database state."""
        stmt = """
            SELECT symbol, max(date) AS latest_date
            FROM pse.daily_stock_price
            WHERE symbol IN ({{ symbols }})
            GROUP BY symbol;
        """
        df = query(stmt=stmt, parameters={'symbols':','.join([f"'{s}'" for s in self.symbols])})
        self.db_latest_dates = df.set_index('symbol').to_dict(orient='dict')['latest_date']
        return self.db_latest_dates
        
    def _delete_records(self, condition="False") -> None:
        """Deletes records from the table."""
        query(f"DELETE FROM pse.daily_stock_price WHERE {condition};", retrieve_result=False)
        self._get_db_latest_dates()
        
    def _insert_price_data_to_db(self, df: pd.DataFrame, batch_size: int = 3000, verbose: bool = True) -> None:
        """Inserts price data records to the database.

        Parameters
        ----------
        df : pandas.DataFrame
            A DataFrame of price data to insert to the database.

        batch_size : int, default 100
            The number of records to insert at a time.
            
        verbose : bool, default True
            Logs progress messages to console when set to True.

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
                if verbose: print(f'  Inserted {len(rows_to_insert)} records.')
                stmt = INSERT_STMT_TEMPLATE.format(tuples = ',\n           '.join(rows_to_insert))
                query(stmt=stmt, retrieve_result=False)
                rows_to_insert = []
                
        
    def sync_table(self, lookback_days : int = 0, freshness_days : int = 1, num_threads : int = 1) -> None:
        """Updates price data for all companies.

        Parameters
        ----------
        lookback_days : int, default 0
            The number of days in the past to re-extract price data for.

        freshness_days : int, default 1
            The acceptable data delay in number of days. By default, this is set to 1
            which means data for yesterday is the minimum acceptable value to consider
            that the price data is already up-to-date.
            
        threads : 
            
        Returns
        -------
        None

        """
        
        def sync_symbol(symbol, lookback_days, freshness_days):
            latest_date = self.db_latest_dates.get(symbol, datetime(1970,1,1).date())
            target_end_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            target_start_date = latest_date + timedelta(days=1-lookback_days)
            
            # Fetch data from API
            price_df = get_stock_data(symbol, start_date=target_start_date, end_date=target_end_date)
                
            # Insert to database
            if price_df.shape[0] == 0:
                print(f'Synced price data for: {symbol:6s}  |  No new records. Skipping.')
            else:
                self._insert_price_data_to_db(price_df, verbose=False)
                print(f'Synced price data for: {symbol:6s}  |  Inserted {price_df.shape[0]} records.')
                
                
        parallel_execute(func = sync_symbol,
                         args = self.symbols,
                         num_threads = num_threads,
                         lookback_days = lookback_days,
                         freshness_days = freshness_days)

        self._get_db_latest_dates()

        
def sync(concurrency=1) -> None:
    """Executes an incremental sync job."""
    
    companies_dataset = PSECompaniesDataset()
    companies_dataset.sync_table()
    
    price_dataset = DailyStockPriceDataset(companies_dataset.source_symbols)
    price_dataset.sync_table(num_threads=concurrency, lookback_days=0)
    
    
def backfill(concurrency=1) -> None:
    """Executes a complete backfill job."""
    
    companies_dataset = PSECompaniesDataset()
    companies_dataset.sync_table()
    
    price_dataset = DailyStockPriceDataset(companies_dataset.source_symbols)
    price_dataset.sync_table(num_threads=concurrency, 
                             lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
    

if __name__ == '__main__':
    
    sync()