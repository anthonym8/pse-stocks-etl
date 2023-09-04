CREATE SCHEMA IF NOT EXISTS `{{ project_id }}.{{ dataset_id }}`;

DROP TABLE IF EXISTS `{{ project_id }}.{{ dataset_id }}.daily_stock_price`;

CREATE TABLE `{{ project_id }}.{{ dataset_id }}.daily_stock_price` (
	  `symbol`         STRING NOT NULL
	, `date`           DATE NOT NULL
	, `open`           FLOAT64
	, `high`           FLOAT64
	, `low`            FLOAT64
	, `close`          FLOAT64
	, `extracted_at`   TIMESTAMP
	, `inserted_at`    TIMESTAMP
)

PARTITION BY `date`
CLUSTER BY symbol
;