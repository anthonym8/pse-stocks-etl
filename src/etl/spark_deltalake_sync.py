"""Module for syncing PSE market data to Delta Lake tables using PySpark"""

# Author: Rey Anthony Masilang


import pandas as pd
import random
import string
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, TimestampType
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

# Custom modules
from pse_edge import get_listed_companies, get_stock_data
from multithreading import parallel_execute


# Load environment variables from file
load_dotenv('.env')

GCP_CREDENTIALS_FILE = os.environ.get('GCP_CREDENTIALS_FILE')
DELTA_TABLE_PATH_PREFIX = os.environ.get('DELTA_TABLE_PATH_PREFIX')
DEFAULT_CONCURRENCY = 8


# Stop default SparkSession before creating another with custom config
try:
    spark.stop()
except:
    pass

# Set DeltaLake-required configuration parameters
spark_builder = SparkSession.builder \
    .appName("ETL") \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

# Create new spark session
spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

    
# Set GCS-related config parameters
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.json.keyfile", GCP_CREDENTIALS_FILE)


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
        
        
def delete_folder(folder_path):
    """Deletes a folder including all subfolders and files."""
    try:
        # Walk through the folder recursively
        for root, dirs, files in os.walk(folder_path, topdown=False):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                os.rmdir(dir_path)

        # Remove the top-level folder itself
        os.rmdir(folder_path)
        print(f"Folder '{folder_path}' and its contents have been deleted.")
    except Exception as e:
        print(f"Error: {e}")


class PSECompaniesDataset:
    """Dataset class for PSE-listed companies."""
    
    def __init__(self):
        self.table_name = 'company'
        self.table_path = f'{DELTA_TABLE_PATH_PREFIX}/{self.table_name}'
        self.schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("company_name", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("subsector", StringType(), True),
            StructField("listing_date", DateType(), True),
            StructField("extracted_at", TimestampType(), True)
        ])
        self._refresh_metadata()
        
    def _create_delta_table(self) -> None:
        """Initializes the Delta table."""
        empty_df = spark.createDataFrame([], schema=self.schema)
        empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.table_path)
        self._refresh_metadata()
        
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based on database state."""
        self.is_delta_table = DeltaTable.isDeltaTable(spark, self.table_path)
        if self.is_delta_table == False:
            self._create_delta_table()
        else:
            self.delta_table = DeltaTable.forPath(spark, self.table_path)
            self.spark_df = self.delta_table.toDF()
            self.spark_df.createOrReplaceTempView(self.table_name)
            self.symbols = self.spark_df.select('symbol').toPandas().symbol.tolist()
            
    def _delete_table_records(self) -> None:
        """Deletes all records in the table."""
        self.delta_table.delete("True")
        self._refresh_metadata()
            
    def fetch_table_records(self) -> pd.DataFrame:
        """Fetches all table rows."""
        return self.spark_df.toPandas()
    
    def sync_table(self) -> None:
        """Syncs database table to PSE Edge data."""
        
        # Extract source data
        pandas_df = get_listed_companies()
        pandas_df['listing_date'] = pd.to_datetime(pandas_df['listing_date'])
        pandas_df['extracted_at'] = pd.to_datetime(pandas_df['extracted_at'])
        
        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df, schema=self.schema)

        # Overwrite Delta table
        spark_df.write.format('delta').mode('overwrite').save(self.table_path)
        
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
        self.table_name = 'daily_stock_price'
        self.table_path = f'{DELTA_TABLE_PATH_PREFIX}/{self.table_name}'
        self.schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("date", DateType(), False),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("extracted_at", TimestampType(), True)
        ])
        self.latest_dates = {}
        self._refresh_metadata()
        
    def _create_delta_table(self) -> None:
        """Initializes the Delta table."""
        empty_df = spark.createDataFrame([], schema=self.schema)
        empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self.table_path)
        self._refresh_metadata()
    
    def _refresh_metadata(self) -> None:
        """Initializes object metadata based on database state."""
        self.is_delta_table = DeltaTable.isDeltaTable(spark, self.table_path)
        if self.is_delta_table == False:
            self._create_delta_table()
        else:
            self.delta_table = DeltaTable.forPath(spark, self.table_path)
            self.spark_df = self.delta_table.toDF()
            self.spark_df.createOrReplaceTempView(self.table_name)
            self.latest_dates = self.spark_df \
                .groupBy('symbol') \
                .agg({'date':'max'}) \
                .withColumnRenamed('max(date)','latest_date') \
                .toPandas() \
                .set_index('symbol') \
                .to_dict(orient='dict') \
                .get('latest_date')
        
    def _delete_table_records(self) -> None:
        """Deletes all records in the table."""
        self.delta_table.delete("True")
                            
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
        
        # Generate unique object key suffix (for uniqueness)
        timestamp_id = datetime.now().strftime('%Y%m%dT%H%M%S')
        unique_id = ''.join(random.choice(string.ascii_letters) for _ in range(8))
        job_output_directory = f'data/tmp/{timestamp_id}_{unique_id}'
        
        # Create temp data folder if it doesn't exist yet
        prepare_directory(f'{job_output_directory}/')
        
        def download_price_updates(symbol, lookback_days, freshness_days):
            """Fetches incremental price data updates."""
            latest_date = pd.to_datetime(self.latest_dates.get(symbol, '1970-01-01')).date()
            target_end_date = (datetime.utcnow() + timedelta(hours=8)).date() - timedelta(days=freshness_days)
            target_start_date = latest_date + timedelta(days=1-lookback_days)
            
            # Fetch data from API
            price_df = get_stock_data(symbol, start_date=target_start_date, end_date=target_end_date)
            
            # Deduplicate records
            price_df = price_df.loc[price_df.groupby(['date','symbol'])['close'].idxmax()]
                
            # Save to CSV
            if price_df.shape[0] == 0:
                print(f'Fetched price data for: {symbol:6s}  |  No price updates available. Skipping.')
            else:
                price_df.to_csv(f'{job_output_directory}/{symbol}.csv', index=False)
                print(f'Fetched price data for: {symbol:6s}  |  Wrote {price_df.shape[0]} records to CSV.')

        # Fetch new price data for all symbols
        parallel_execute(func = download_price_updates,
                         args = self.symbols,
                         num_threads = num_threads,
                         lookback_days = lookback_days,
                         freshness_days = freshness_days)
        
        # Load complete dataset to Spark DataFrame
        try:
            updates_df = spark.read.csv(f'{job_output_directory}/*.csv', header=True, inferSchema=True)        
        
            # Merge updates to Delta table
            self.delta_table.alias("target") \
                .merge(updates_df.alias("source"), "source.symbol=target.symbol AND source.date = target.date") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        
            # Refresh object metadata
            self._refresh_metadata()
            
        except AnalysisException as e:
            print('No updates available. Skipping upsert step.')

        # Clean up temporary CSV files
        delete_folder(job_output_directory)
        
        
def sync(concurrency=DEFAULT_CONCURRENCY) -> None:
    """Executes an incremental sync job."""
    
    pse_companies = PSECompaniesDataset()
    pse_companies.sync_table()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_table(num_threads=concurrency)
    
    
def backfill(concurrency=DEFAULT_CONCURRENCY) -> None:
    """Executes a complete backfill job."""
    
    pse_companies = PSECompaniesDataset()
    pse_companies.sync_table()
    
    price_dataset = DailyStockPriceDataset(pse_companies.symbols)
    price_dataset.sync_table(num_threads=concurrency, 
                             lookback_days=365*100)  # Use a very large lookback period (100 years) to extract all available data
        

if __name__ == '__main__':
    
    sync()