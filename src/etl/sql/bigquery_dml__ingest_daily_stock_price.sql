-- Create staging table
CREATE OR REPLACE TABLE {{ ingest_table }} AS
    SELECT * FROM {{ target_table }} WHERE False;

LOAD DATA INTO {{ ingest_table }}
    FROM FILES(
        skip_leading_rows=1,
        format='CSV',
        uris = ['gs://{{ bucket_name }}/{{ job_output_directory }}/*']
    );         
