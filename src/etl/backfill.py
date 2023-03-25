"""Script for backfilling market data for PSE stocks"""

# Author: Rey Anthony Masilang


from src.etl.sync import backfill


if __name__ == '__main__':
    backfill()