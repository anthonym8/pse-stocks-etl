CREATE SCHEMA IF NOT EXISTS `{{ project_id }}.{{ dataset_id }}`;

CREATE OR REPLACE TABLE `{{ project_id }}.{{ dataset_id }}.company` (
	  `symbol`         STRING
	, `name`           STRING
	, `sector`         STRING
	, `subsector`      STRING
	, `listing_date`   DATE
	, `extracted_at`   TIMESTAMP
	, `inserted_at`    TIMESTAMP
);
