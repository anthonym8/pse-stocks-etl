BEGIN;

-- Create staging table
CREATE OR REPLACE TEMPORARY TABLE daily_stock_price__tmp_table__rows_to_insert AS
    SELECT * FROM {{ target_table }} WHERE False;

-- Filter rows to update
INSERT INTO daily_stock_price__tmp_table__rows_to_insert
    (symbol, `date`, open, high, low, close, extracted_at, inserted_at)
    SELECT source.*
    FROM {{ ingest_table }} AS source
        LEFT JOIN {{ target_table }} AS target
            ON target.symbol = source.symbol
            AND target.`date` = source.`date`
    WHERE source.extracted_at >= target.extracted_at
       OR target.extracted_at IS NULL;

-- Delete outdated rows from target table
DELETE FROM {{ target_table }}
WHERE symbol || '::' || `date` IN ( SELECT symbol || '::' || `date` FROM daily_stock_price__tmp_table__rows_to_insert );

-- Insert updated rows
INSERT INTO {{ target_table }}
    (symbol, `date`, open, high, low, close, extracted_at, inserted_at)
    SELECT symbol
         , `date`
         , open
         , high
         , low
         , close
         , extracted_at
         , CURRENT_TIMESTAMP()
    FROM daily_stock_price__tmp_table__rows_to_insert;

COMMIT;          