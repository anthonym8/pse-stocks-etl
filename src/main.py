"""Main ETL script"""

# Author: Rey Anthony Masilang


import argparse
from src.db.postgres.init import create_tables as postgres_initdb
from src.etl.postgres_sync import sync as postgres_sync
from src.etl.postgres_sync import backfill as postgres_backfill


DB_OPTIONS = ['postgres']

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
    
    
    if args.destination == 'postgres':
        if args.action == 'initdb':
            postgres_initdb()
        elif args.action == 'backfill':
            postgres_backfill(concurrency=args.concurrency)
        elif args.action == 'sync':
            postgres_sync(concurrency=args.concurrency)