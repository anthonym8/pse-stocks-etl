"""Module for syncing PSE market data to Delta Lake tables"""

# Author: Rey Anthony Masilang


import pandas as pd
import pyarrow as pa
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv
from os import environ
from typing import List
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError
from src.utils.pse_edge import get_listed_companies, get_stock_data
from src.utils.multithreading import parallel_execute
from src.utils.gcs import list_objects, delete_object


load_dotenv('.env')

GCP_CREDENTIALS_FILE = environ.get('GCP_CREDENTIALS_FILE')
GCS_BUCKET_NAME = environ.get('GCS_BUCKET_NAME')

storage_options = {"GOOGLE_SERVICE_ACCOUNT_PATH":GCP_CREDENTIALS_FILE, 
                   "GOOGLE_BUCKET_NAME":GCS_BUCKET_NAME}


class PSECompaniesDataset:
    """Dataset class for PSE-listed companies."""
    
    def __init__(self):
        self.table_directory = 'delta-lake/pse/company'
        self.table_path = f'gs://{GCS_BUCKET_NAME}/{self.table_directory}'
        self._refresh_metadata()
        
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        try:
            self.delta_table = DeltaTable(self.table_path, storage_options=storage_options)
            symbols_df = self.delta_table.to_pandas(columns=['symbol'])
            self.symbols = symbols_df.symbol.tolist()  # List of symbols

        except TableNotFoundError as e:
            self.delta_table = None
            self.symbols = []
        
    def _delete_delta_table(self) -> None:
        """Deletes the Delta table from cloud storage"""
        table_artifact_uris = list_objects(bucket_name=GCS_BUCKET_NAME, prefix=self.table_directory)
        for key in table_artifact_uris:
            delete_object(bucket_name=GCS_BUCKET_NAME, object_key=key)
            
    def fetch_table_records(self) -> pd.DataFrame:
        """Fetches the current database records."""
        return self.delta_table.to_pandas()
    
    def sync_table(self) -> None:
        """Syncs database table to PSE Edge data."""
        
        # Extract source data
        df = get_listed_companies()
        df = df.astype({'symbol':str,
                        'company_name':str,
                        'sector':str,
                        'subsector':str,
                        'listing_date':'datetime64',
                        'extracted_at':'datetime64'})

        # Write to Delta table
        write_deltalake(self.table_path, data=df, storage_options=storage_options, mode='overwrite', overwrite_schema=True)
        
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
        self.table_directory = 'delta-lake/pse/daily_stock_price'
        self.table_path = f'gs://{GCS_BUCKET_NAME}/{self.table_directory}'
        self.pandas_schema = {'symbol':str,
                              'date':str,
                              'open':float,
                              'high':float,
                              'low':float,
                              'close':float,
                              'extracted_at':str}
        self.pyarrow_schema = pa.schema([('symbol', pa.string()),
                                         ('date', pa.string()),
                                         ('open', pa.float64()),
                                         ('high', pa.float64()),
                                         ('low', pa.float64()),
                                         ('close', pa.float64()),
                                         ('extracted_at', pa.string())])
        self._refresh_metadata()
    
    def _get_latest_dates(self) -> dict:
        """Fetches the current database state."""
        if self.delta_table is not None:
            duckdb_table = duckdb.arrow(self.delta_table.to_pyarrow_dataset())
            result = duckdb_table.query(virtual_table_name='daily_stock_price',
                                        sql_query='SELECT symbol, max(date) AS latest_date FROM daily_stock_price GROUP BY symbol ORDER BY symbol')
            df = result.to_df()
            latest_dates = df.set_index('symbol').to_dict(orient='dict')['latest_date']
            latest_dates = {k:pd.to_datetime(v).date() for k,v in latest_dates.items()}
        else:
            latest_dates = {s:None for s in self.symbols}            
        
        return latest_dates
    
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        try:
            self.delta_table = DeltaTable(self.table_path, storage_options=storage_options)
        except TableNotFoundError as e:
            self.delta_table = None
            
        latest_dates = self._get_latest_dates()
        self.latest_dates = {s:latest_dates.get(s, None) for s in self.symbols}
        
    def _delete_delta_table(self) -> None:
        """Deletes the Delta table from cloud storage"""
        table_artifact_uris = list_objects(bucket_name=GCS_BUCKET_NAME, prefix=self.table_directory)
        for key in table_artifact_uris:
            delete_object(bucket_name=GCS_BUCKET_NAME, object_key=key)
                    
    def sync_table(self, freshness_days : int = 1, num_threads : int = 1) -> None:
        """Updates price data for all companies.

        Parameters
        ----------
        freshness_days : int, default 1
            The acceptable data delay in number of days. By default, this is set to 1
            which means data for yesterday is the minimum acceptable value to consider
            that the price data is already up-to-date.
            
        num_threads : int, default 1
            The number of threads in parallel to use.
            
        Returns
        -------
        None

        """
        
        def sync_symbol(symbol, freshness_days):
            
            target_latest_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            latest_date = self.latest_dates.get(symbol, None)
            
            # Skip data extraction if conditions are satisfied
            if latest_date is not None and latest_date>=target_latest_date:
                # Return empty data frame.
                price_df = pd.DataFrame(columns=['symbol','date','open','high','low','close','extracted_at'])

            # Extract new price data
            else:
                if latest_date is not None: 
                    start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    start_date = None

                price_df = get_stock_data(symbol, start_date=start_date, end_date=target_latest_date)
                price_df = price_df.astype(self.pandas_schema)
                
            # Insert to Delta table
            if price_df.shape[0] == 0:
                print(f'Synced price data for: {symbol:6s}  |  No new records. Skipping.')
            else:
                # Append new rows to table partition
                write_deltalake(self.table_path, data=price_df, storage_options=storage_options, 
                                partition_by=['symbol'], mode='append', schema=self.pyarrow_schema)
                print(f'Synced price data for: {symbol:6s}  |  Inserted {price_df.shape[0]} records.')
                
                
        parallel_execute(func = sync_symbol,
                         args = self.symbols,
                         num_threads = num_threads,
                         freshness_days = freshness_days)

        self._refresh_metadata()
        
    def backfill_table(self, freshness_days : int = 1, num_threads : int = 1) -> None:
        """Backfills price data for all companies.

        Parameters
        ----------
        freshness_days : int, default 1
            The acceptable data delay in number of days. By default, this is set to 1
            which means data for yesterday is the minimum acceptable value to consider
            that the price data is already up-to-date.
            
        num_threads : int, default 1
            The number of threads in parallel to use.
            
        Returns
        -------
        None

        """
        
        def backfill_symbol(symbol, freshness_days):
            target_latest_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            price_df = get_stock_data(symbol, start_date=None, end_date=target_latest_date)
                
            # Insert to Delta table
            if price_df.shape[0] == 0:
                print(f'Synced price data for: {symbol:6s}  |  No new records. Skipping.')
            else:
                # Append new rows to table partition
                write_deltalake(self.table_path, data=price_df, storage_options=storage_options, 
                                partition_by=['symbol'], mode='overwrite', schema=self.pyarrow_schema,
                                partition_filters=[('symbol','=',symbol)])
                print(f'Synced price data for: {symbol:6s}  |  Inserted {price_df.shape[0]} records.')
                
        parallel_execute(func = sync_symbol,
                         args = self.symbols,
                         num_threads = num_threads,
                         freshness_days = freshness_days)

        self._refresh_metadata()

        
def sync(concurrency=1) -> None:
    """Executes an incremental sync job."""
    
    pse_companies = PSECompaniesDataset()
    pse_companies.sync_table()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_table(num_threads=concurrency)
    
    
def backfill(concurrency=1) -> None:
    """Executes a complete backfill job."""
    
    pse_companies = PSECompaniesDataset()
    pse_companies.sync_table()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.backfill_table(num_threads=concurrency)
    

if __name__ == '__main__':
    
    sync()