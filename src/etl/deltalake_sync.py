"""Module for syncing PSE market data to Delta Lake tables"""

# Author: Rey Anthony Masilang


import pandas as pd
import polars as pl
import random
import string
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
from src.utils.misc import prepare_directory, delete_files


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
        self.polars_schema = {'symbol':pl.Utf8,
                              'company_name':pl.Utf8,
                              'sector':pl.Utf8,
                              'subsector':pl.Utf8,
                              'listing_date':pl.Date,
                              'extracted_at':pl.Datetime}
        self._refresh_metadata()
        
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        try:
            self.delta_table = DeltaTable(self.table_path, storage_options=storage_options)
            symbols_query = "SELECT DISTINCT symbol FROM company ORDER BY symbol;"
            duckdb_table = duckdb.arrow(self.delta_table.to_pyarrow_dataset())
            symbols_df = duckdb_table.query(virtual_table_name='company', sql_query=symbols_query).df()
            self.symbols = symbols_df.loc[:,'symbol'].tolist()

        except TableNotFoundError as e:
            self.delta_table = None
            self.symbols = []
        
    def _delete_delta_table(self) -> None:
        """Deletes the Delta table from cloud storage"""
        table_artifact_uris = list_objects(bucket_name=GCS_BUCKET_NAME, prefix=self.table_directory)
        for key in table_artifact_uris:
            delete_object(bucket_name=GCS_BUCKET_NAME, object_key=key)
    
    def sync_table(self) -> None:
        """Syncs database table to PSE Edge data."""
        
        # Generate unique folder name (for uniqueness)
        timestamp_id = datetime.now().strftime('%Y%m%dT%H%M%S')
        unique_id = ''.join(random.choice(string.ascii_letters) for _ in range(8))
        file_path = f'data/tmp/company/{timestamp_id}_{unique_id}.csv'

        # Extract source data
        df = get_listed_companies()
        
        # Save to CSV and read to Polars dataframe (for column type correctness)
        prepare_directory(file_path)
        df.to_csv(file_path, index=False)
        polars_df = pl.read_csv(file_path, schema=self.polars_schema)
        
        # Write to Delta table
        polars_df.write_delta(self.table_path, storage_options=storage_options, mode='overwrite', overwrite_schema=True)        
        self._refresh_metadata()
        
        # Vacuum Delta table (remove obsolete files)
        self.delta_table.vacuum(dry_run=False, retention_hours=0, enforce_retention_duration=False)
        
        # Delete local artifacts
        delete_files([file_path])
        

class DailyStockPriceDataset:
    """Dataset class for the daily stock price table.
    
    Parameters
    ----------
    symbols : str
        A list of ticker symbols for PSE-listed companies.
    
    """
    
    def __init__(self, symbols : List[str]):
        self.symbols = symbols
        self.table_directory = 'delta-lake/pse/daily_stock_price'
        self.table_path = f'gs://{GCS_BUCKET_NAME}/{self.table_directory}'
        self.polars_schema = {'symbol':pl.Utf8,
                              'date':pl.Date,
                              'open':pl.Float32,
                              'high':pl.Float32,
                              'low':pl.Float32,
                              'close':pl.Float32,
                              'extracted_at':pl.Datetime}
        
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
            
        num_threads : int, default 1
            The number of threads in parallel to use.
            
        Returns
        -------
        None

        """
                
        # Generate unique folder name (for uniqueness)
        timestamp_id = datetime.now().strftime('%Y%m%dT%H%M%S')
        unique_id = ''.join(random.choice(string.ascii_letters) for _ in range(8))
        job_output_directory = f'data/tmp/price/{timestamp_id}_{unique_id}'
        
        csv_files = []

        def extract_price_updates(symbol, freshness_days):
            
            target_latest_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            latest_date = self.latest_dates.get(symbol, None)
            
            # Skip data extraction if conditions are satisfied
            if latest_date is not None and latest_date>=target_latest_date:
                # Return empty data frame.
                price_df = pd.DataFrame(columns=self.polars_schema.keys())

            # Extract new price data
            else:
                if latest_date is not None: 
                    start_date = (latest_date + timedelta(days=1-lookback_days)).strftime('%Y-%m-%d')
                else:
                    start_date = None

                # Extract data
                price_df = get_stock_data(symbol, start_date=start_date, end_date=target_latest_date)
                
                # Deduplicate rows
                price_df = duckdb.sql("SELECT * FROM price_df \
                                       QUALIFY row_number() OVER (partition by symbol, date order by date) = 1;").df()
                
            # Insert to Delta table
            if price_df.shape[0] == 0:
                print(f'No new price data for: {symbol:6s}  |  Skipping.')
            else:
                # Save to CSV
                file_path = f'{job_output_directory}/{symbol}.csv'
                prepare_directory(file_path)
                price_df.to_csv(file_path, index=False)
                csv_files.append(file_path)
                print(f'Downloaded price data for: {symbol:6s}  |  {price_df.shape[0]} records.')

        # Download price updates
        parallel_execute(func = extract_price_updates,
                         args = self.symbols,
                         num_threads = num_threads,
                         freshness_days = freshness_days)
        
        try:
            # Read CSVs to Polars dataframe
            updates_df = pl.read_csv(f'{job_output_directory}/*.csv', schema=self.polars_schema)
            
            # Insert to Delta table
            updates_df.write_delta(self.table_path, storage_options=storage_options, mode='append')

            # Re-fetch Delta table
            self._refresh_metadata()

            # Optimize Delta table
            self.delta_table.optimize.compact()

            # Vacuum Delta table (remove obsolete files)
            self.delta_table.vacuum(dry_run=False, retention_hours=0, enforce_retention_duration=False)

        except pl.ComputeError as e:
            print('No updates. Done.')
            
        # Clean up local artifacts
        delete_files(csv_files)

        
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
    price_dataset.sync_table(num_threads=concurrency, 
                             lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
    

def delete_tables() -> None:
    """Deletes existing Delta tables."""
    
    pse_companies = PSECompaniesDataset()
    pse_companies._delete_delta_table()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset._delete_delta_table()
    

if __name__ == '__main__':
    
    sync()