"""Module for extracting backfilling market data for PSE stocks"""

# Author: Rey Anthony Masilang


import pandas as pd
from datetime import datetime
from src.data.stocks import get_listed_companies, get_stock_data
from src.etl.postgres import query

def get_ts():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Extract complete list of companies
company_extract_ts = get_ts()
companies_df = get_listed_companies()
n_symbols = companies_df.shape[0]

# Load to database
print('Inserting rows to database.')

BATCH_SIZE = 100
INSERT_STMT_TEMPLATE = r"""
    INSERT INTO pse.company (symbol, company_name, sector, subsector, listing_date, extracted_at)
    VALUES {}
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
for idx in range(n_symbols):
    symbol = companies_df.iloc[idx].loc['symbol']
    company_name = companies_df.iloc[idx].loc['company_name']
    sector = companies_df.iloc[idx].loc['sector']
    subsector = companies_df.iloc[idx].loc['subsector']
    listing_date = companies_df.iloc[idx].loc['listing_date']
    extracted_at = company_extract_ts
    
    row_str = "('{}','{}','{}','{}','{}','{}')".format(
        symbol, 
        company_name.replace('\'','\'\''), 
        sector, 
        subsector, 
        listing_date,
        extracted_at
    )
    
    rows_to_insert.append(row_str)
    
    if (len(rows_to_insert) == BATCH_SIZE) or (idx+1 == n_symbols):
        print('  {} out of {}'.format(idx+1, n_symbols))
        stmt = INSERT_STMT_TEMPLATE.format(',\n           '.join(rows_to_insert))
        query(stmt=stmt, retrieve_result=False)

        rows_to_insert = []

        
        
    
# Extract and load price data
BATCH_SIZE = 3000
INSERT_STMT_TEMPLATE = r"""
    INSERT INTO pse.daily_stock_price (symbol, date, open, high, low, close, extracted_at)
    VALUES {}
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

    
# Extract all available price data for each company
for idx in range(n_symbols):
    symbol = companies_df.iloc[idx].loc['symbol']
    print('Extracting price data:  {:6s}  ({} out of {})'.format(symbol, idx+1, n_symbols))
    
    price_extract_ts = get_ts()
    price_df = get_stock_data(symbol)
    
    # Insert to database
    n_rows = price_df.shape[0]
    
    if n_rows == 0:
        print('  No price data available. Skipping.')
        
    else:
        print('  Inserting rows to database.')

        rows_to_insert = []
        for idy in range(n_rows):
            p_symbol = price_df.iloc[idy].loc['symbol']
            p_date = price_df.iloc[idy].loc['date']
            p_open = price_df.iloc[idy].loc['open']
            p_high = price_df.iloc[idy].loc['high']
            p_low = price_df.iloc[idy].loc['low']
            p_close = price_df.iloc[idy].loc['close']
            p_extracted_at = price_extract_ts

            row_str = "('{}', '{}', {}, {}, {}, {}, '{}')".format(
                p_symbol, 
                p_date, 
                p_open, 
                p_high, 
                p_low, 
                p_close, 
                p_extracted_at
            )

            rows_to_insert.append(row_str)

            if (len(rows_to_insert) == BATCH_SIZE) or (idy+1 == n_rows):
                print('    {:6s}  |  {}  | {} out of {}'.format(symbol, p_date, idy+1, n_rows))
                stmt = INSERT_STMT_TEMPLATE.format(',\n           '.join(rows_to_insert))
                query(stmt=stmt, retrieve_result=False)

                rows_to_insert = []

