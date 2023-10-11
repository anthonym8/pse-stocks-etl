"""Module for syncing PSE market data to a Postgres database"""

# Author: Rey Anthony Masilang


import pandas as pd
from datetime import datetime, timedelta
from src.utils.pse_edge import get_listed_companies, get_stock_data, UnknownSymbolException
from src.utils.postgres import query
from src.utils.multithreading import parallel_execute
from typing import List
from src import logger
    
    
class PSECompaniesDataset:
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
        logger.info('Inserting rows to database.')

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
                logger.info(f'  {idx+1} out of {n_companies}')
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
        self.latest_dates = self._get_latest_dates()
        
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
        for idx in range(n_rows):
            p_symbol, p_date, p_open, p_high, p_low, p_close, p_extracted_at = \
                df.iloc[idx].loc[['symbol','date','open','high','low','close','extracted_at']]

            row_str = f"('{p_symbol}', '{p_date}', {p_open}, {p_high}, {p_low}, {p_close}, '{p_extracted_at}')"
            rows_to_insert.append(row_str)

            if (len(rows_to_insert) == batch_size) or (idx+1 == n_rows):
                logger.info(f"  {idx+1} records out of {n_rows} inserted.")
                stmt = INSERT_STMT_TEMPLATE.format(tuples = ',\n           '.join(rows_to_insert))
                query(stmt=stmt, retrieve_result=False)
                rows_to_insert = []
                
        
    def sync_db(self, lookback_days : int = 0, freshness_days : int = 1, num_threads : int = 1) -> None:
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
        
        def sync_symbol(symbol, lookback_days, freshness_days, latest_dates_dict):
            """Extract and load price data per symbol."""
            
            # Compute target start and end dates
            current_end_date = latest_dates_dict.get(symbol, datetime(1970,1,1).date())
            target_start_date = current_end_date + timedelta(days=1-lookback_days)
            target_end_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            
            # Skip if conditions are satisfied
            if lookback_days==0 and current_end_date==target_end_date:
                logger.info(f'Table is up-to-date:   {symbol:6s}  |  Skipping.')

            # Else, run sync job
            else:
                try:
                    # Fetch data from API
                    price_df = get_stock_data(symbol, start_date=target_start_date, end_date=target_end_date)

                    # Deduplicate records
                    price_df = price_df.loc[price_df.groupby(['date','symbol'])['close'].idxmax()]
                    
                    # Insert to database
                    if price_df.shape[0] > 0:
                        self._insert_price_data_to_db(price_df)
                        logger.info(f'Synced price data for: {symbol:6s}  |  Inserted {price_df.shape[0]} records.')
                    else:
                        logger.info(f'Zero records returned: {symbol:6s}  |  Skipping.')

                except UnknownSymbolException as e:
                    logger.warning(f'Error: Unknown symbol: {symbol:6s}  |  Skipping.')
                
                
        parallel_execute(func = sync_symbol,
                         args = self.symbols,
                         num_threads = num_threads,
                         lookback_days = lookback_days,
                         freshness_days = freshness_days,
                         latest_dates_dict = self.latest_dates)

        self._refresh_metadata()

        
def sync(concurrency=1) -> None:
    """Executes an incremental sync job."""
    
    logger.info("Syncing companies data...")
    pse_companies = PSECompaniesDataset()
    pse_companies.sync_db()
    
    logger.info("Syncing price data...")
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_db(num_threads=concurrency, lookback_days=0)
    
    logger.info("Done.")
    
    
def backfill(concurrency=1) -> None:
    """Executes a complete backfill job."""
    
    logger.info("Running backfill for companies data...")
    pse_companies = PSECompaniesDataset()
    pse_companies.sync_db()
    
    logger.info("Running backfill for price data...")
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_db(num_threads=concurrency, 
                          lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
    
    logger.info("Done.")
    
if __name__ == '__main__':
    
    sync()