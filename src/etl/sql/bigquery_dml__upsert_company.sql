CREATE OR REPLACE TEMPORARY TABLE company__etl_tmp AS
    SELECT * FROM {{ project_id }}.{{ dataset_id }}.company WHERE False;

INSERT INTO company__etl_tmp (symbol, company_name, sector, subsector, listing_date, extracted_at)
VALUES {{ tuples }};

MERGE INTO {{ project_id }}.{{ dataset_id }}.company AS target
USING ( SELECT * FROM company__etl_tmp ) AS source
ON target.symbol = source.symbol
WHEN MATCHED THEN 
    UPDATE SET
    target.symbol = source.symbol,
    target.company_name = source.company_name,
    target.sector = source.sector,
    target.subsector = source.subsector,
    target.listing_date = source.listing_date,
    target.extracted_at = source.extracted_at,
    target.inserted_at = '{{ current_timestamp }}'
WHEN NOT MATCHED THEN
    INSERT (symbol, company_name, sector, subsector, listing_date, extracted_at, inserted_at)
    VALUES (source.symbol, 
            source.company_name, 
            source.sector, 
            source.subsector, 
            source.listing_date, 
            source.extracted_at,
            '{{ current_timestamp }}');
