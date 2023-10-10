"""Main ETL script"""

# Author: Rey Anthony Masilang


import argparse

from src import logger

from src.db.postgres.init import create_tables as postgres_initdb
from src.etl.postgres_sync import sync as postgres_sync
from src.etl.postgres_sync import backfill as postgres_backfill

from src.db.bigquery.init import create_tables as bigquery_initdb
from src.etl.bigquery_sync import sync as bigquery_sync
from src.etl.bigquery_sync import backfill as bigquery_backfill

from src.etl.deltalake_sync import delete_tables as deltalake_reset
from src.etl.deltalake_sync import sync as deltalake_sync
from src.etl.deltalake_sync import backfill as deltalake_backfill


DB_OPTIONS = ['postgres', 'bigquery', 'deltalake']

ACTIONS = [ 'initdb',    # Initialize database
            'backfill',  # Backfill dataset
            'sync'       # Incremental sync
          ]


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='PSE Stocks ETL')
    parser.add_argument('--destination', '-d', default='postgres', choices=DB_OPTIONS, help="The destination database where data will be stored.")
    parser.add_argument('--action', '-a', default='sync', choices=ACTIONS, help="The operation to execute, e.g. initdb, backfill, or sync.")
    parser.add_argument('--concurrency', '-c', default=1, type=int, help="The number of threads in parallel to use.")

    args = parser.parse_args()
    
    logger.info(f"Input args: destination='{args.destination}', action='{args.action}', concurrency='{args.concurrency}'")
    
    
    if args.destination == 'postgres':
        if args.action == 'initdb':
            postgres_initdb()
        elif args.action == 'backfill':
            postgres_backfill(concurrency=args.concurrency)
        elif args.action == 'sync':
            postgres_sync(concurrency=args.concurrency)
            
            
    elif args.destination == 'bigquery':
        if args.action == 'initdb':
            bigquery_initdb()
        elif args.action == 'backfill':
            bigquery_backfill(concurrency=args.concurrency)
        elif args.action == 'sync':
            bigquery_sync(concurrency=args.concurrency)
            
            
    elif args.destination == 'deltalake':
        if args.action == 'initdb':
            deltalake_reset()
        elif args.action == 'backfill':
            deltalake_backfill(concurrency=args.concurrency)
        elif args.action == 'sync':
            deltalake_sync(concurrency=args.concurrency)
