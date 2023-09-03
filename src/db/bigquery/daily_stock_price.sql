CREATE SCHEMA IF NOT EXISTS `{{ project_id }}.{{ dataset_id }}`;

CREATE OR REPLACE TABLE `{{ project_id }}.{{ dataset_id }}.daily_stock_price` (
	  `symbol`         STRING
	, `date`           DATE
	, `open`           FLOAT64
	, `high`           FLOAT64
	, `low`            FLOAT64
	, `close`          FLOAT64
	, `extracted_at`   TIMESTAMP
	, `inserted_at`    TIMESTAMP
);