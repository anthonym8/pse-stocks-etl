"""Module for syncing PSE market data to BigQuery"""

# Author: Rey Anthony Masilang


import pandas as pd
import random
import string
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from os import environ
from typing import List
from src.utils.pse_edge import get_listed_companies, get_stock_data, UnknownSymbolException
from src.utils.bigquery import execute, query
from src.utils.multithreading import parallel_execute
from src.utils.misc import prepare_directory, delete_files
from src.utils.gcs import upload_to_gcs, delete_object


load_dotenv('.env')

GCP_PROJECT_ID = environ.get('GCP_PROJECT_ID')
GCS_BUCKET_NAME = environ.get('GCS_BUCKET_NAME')
BIGQUERY_DATASET_ID = environ.get('BIGQUERY_DATASET_ID')



class PSECompanies:
    """Dataset class for PSE-listed companies."""
    
    def __init__(self):
        self._refresh_metadata()
        
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        symbols_df = query(f"SELECT symbol FROM {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.company ORDER BY symbol;")
        self.symbols = symbols_df.symbol.tolist()  # List of symbols
        
    def _delete_all_records(self) -> None:
        """Deletes all database records."""
        execute(f"DELETE FROM {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.company WHERE True;")
        self._refresh_metadata()
    
    def fetch_db_records(self) -> pd.DataFrame:
        """Fetches the current database records."""
        data = query(f"SELECT * FROM {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.company ORDER BY symbol;")
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
        rows_to_insert = []
        n_companies = source_data.shape[0]
        for idx in range(n_companies):
            symbol, company_name, sector, subsector, listing_date, extracted_at = \
                source_data.iloc[idx].loc[['symbol', 'company_name', 'sector', 'subsector', 'listing_date', 'extracted_at']]

            row_str = f"('{symbol}','{company_name}','{sector}','{subsector}','{listing_date}','{extracted_at}')"
            row_str = row_str.replace("''", "\\'")  # escape single quotes
            rows_to_insert.append(row_str)

            if (len(rows_to_insert) == batch_size) or (idx+1 == n_companies):
                print(f'  {idx+1} out of {n_companies}')
                parameters = {
                    'project_id': GCP_PROJECT_ID,
                    'dataset_id': BIGQUERY_DATASET_ID,
                    'tuples': ',\n           '.join(rows_to_insert),
                    'current_timestamp': datetime.utcnow().isoformat()
                }
                execute(sql_file='src/etl/sql/bigquery_dml__upsert_company.sql', parameters=parameters)
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
        sql_statement = """
            SELECT symbol, max(date) AS latest_date
            FROM `{{ project_id }}.{{ dataset_id }}.daily_stock_price`
            GROUP BY symbol;
        """
        params = {'project_id':GCP_PROJECT_ID, 'dataset_id':BIGQUERY_DATASET_ID}

        df = query(sql_statement=sql_statement, parameters=params)
        latest_dates = df.set_index('symbol').to_dict(orient='dict')['latest_date']
        
        return latest_dates
    
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based from database state."""
        self.latest_dates = self._get_latest_dates()
        
    def _delete_all_records(self) -> None:
        """Deletes all database records."""
        execute(sql_statement="DELETE FROM `{{ project_id }}.{{ dataset_id }}.daily_stock_price` WHERE True;",
                parameters={'project_id':GCP_PROJECT_ID, 'dataset_id':BIGQUERY_DATASET_ID})
        self._refresh_metadata()
                
        
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
        
        gcs_uploaded_files = []
        
        # Generate unique object key suffix (for uniqueness)
        timestamp_id = datetime.now().strftime('%Y%m%dT%H%M%S')
        unique_id = ''.join(random.choice(string.ascii_letters) for _ in range(8))
        job_output_directory = f'data/{timestamp_id}_{unique_id}'
        
        # Create directory
        prepare_directory(f'{job_output_directory}/')
        
        # Prepare BigQuery table identifiers
        target_table_name = f'`{GCP_PROJECT_ID}`.`{BIGQUERY_DATASET_ID}`.daily_stock_price'
        ingest_table_name = f'`{GCP_PROJECT_ID}`.`{BIGQUERY_DATASET_ID}`.daily_stock_price__tmp_ingest__{timestamp_id}_{unique_id}'
                
        def sync_symbol_data_to_gcs(symbol, lookback_days, freshness_days, latest_dates_dict):
            """Extract data from PSE Edge and upload to GCS"""
            
            # Compute target start and end dates
            current_end_date = latest_dates_dict.get(symbol, datetime(1970,1,1).date())
            target_start_date = current_end_date + timedelta(days=1-lookback_days)
            target_end_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
                        
            # Skip if conditions are satisfied
            if lookback_days==0 and current_end_date==target_end_date:
                print(f'Synced price data for: {symbol:6s}  |  No new records. Skipping.')

            else:
                try:
                    # Fetch data from API
                    price_df = get_stock_data(symbol, start_date=target_start_date, end_date=target_end_date)

                    # Deduplicate records
                    price_df = price_df.loc[price_df.groupby(['date','symbol'])['close'].idxmax()]
                    
                    # Upload CSV to GCS
                    if price_df.shape[0] > 0:
                        # Save to CSV
                        file_path = f'{job_output_directory}/{symbol}.csv'
                        price_df.to_csv(file_path, index=False)

                        # Upload to GCS
                        upload_to_gcs(source_file_path=file_path, bucket_name=GCS_BUCKET_NAME, object_key=file_path)
                        gcs_uploaded_files.append(file_path)

                        # Delete local CSV file
                        delete_files(file_path, verbose=False)
                        print(f'Uploaded CSV to GCS:   {symbol:6s}  |  {price_df.shape[0]} records.')
                    
                    else:
                        print(f'Synced price data for: {symbol:6s}  |  No new records. Skipping.')

                except UnknownSymbolException as e:
                    print(f'Error: Unknown symbol: {symbol:6s}  |  Skipping.')
                
        # Download price updates and upload to GCS
        parallel_execute(func = sync_symbol_data_to_gcs,
                         args = self.symbols,
                         num_threads = num_threads,
                         lookback_days = lookback_days,
                         freshness_days = freshness_days,
                         latest_dates_dict = self.latest_dates)
        
        
        if len(gcs_uploaded_files) > 0:
            
            try:
                # Load new data to staging table
                print(f'Ingesting new records to {ingest_table_name}.')
                execute(sql_file='src/etl/sql/bigquery_dml__ingest_daily_stock_price.sql', 
                        parameters={ 'ingest_table': ingest_table_name,
                                     'target_table': target_table_name,
                                     'bucket_name': GCS_BUCKET_NAME,
                                     'job_output_directory': job_output_directory })

                # Merge data to main table
                print(f'Merging new records to {target_table_name}.')
                execute(sql_file='src/etl/sql/bigquery_dml__upsert_daily_stock_price.sql', 
                        parameters={ 'ingest_table': ingest_table_name,
                                     'target_table': target_table_name })
                
                # Delete temporary ingest table
                print(f'Dropping temporary staging table.')
                execute(sql_statement = f'DROP TABLE IF EXISTS {ingest_table_name}')
                
                # Delete cloud storage objects
                print(f'Deleting temporary objects on GCS.')
                parallel_execute(func = lambda x: delete_object(bucket_name=GCS_BUCKET_NAME, object_key=x),
                                 args = gcs_uploaded_files,
                                 num_threads = num_threads)
                
                print('Done.')

            except Exception as e:  # Clean up in case an exception occurs
                
                # Delete temporary ingest table
                print(f'Dropping temporary staging table.')
                execute(sql_statement = f'DROP TABLE IF EXISTS {ingest_table_name}')
                
                # Delete cloud storage objects
                print(f'Deleting temporary objects on GCS.')
                parallel_execute(func = lambda x: delete_object(bucket_name=GCS_BUCKET_NAME, object_key=x),
                                 args = gcs_uploaded_files,
                                 num_threads = num_threads)
                
                print('Done.')
                raise e


        self._refresh_metadata()
        
        
def sync(concurrency=1) -> None:
    """Executes an incremental sync job."""
    
    pse_companies = PSECompanies()
    pse_companies.sync_db()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_db(num_threads=concurrency, lookback_days=0)

    
def backfill(concurrency=1) -> None:
    """Executes a complete backfill job."""
    
    pse_companies = PSECompanies()
    pse_companies.sync_db()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_db(num_threads=concurrency, 
                          lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data


if __name__ == '__main__':
        
    sync()