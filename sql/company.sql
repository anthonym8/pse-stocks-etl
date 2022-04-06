CREATE SCHEMA IF NOT EXISTS pse;

DROP TABLE IF EXISTS pse.company;

CREATE TABLE IF NOT EXISTS pse.company (
	  "symbol"         VARCHAR(10) NOT NULL PRIMARY KEY
	, "company_name"   VARCHAR(100) NOT NULL
	, "sector"         VARCHAR(100) NOT NULL
	, "subsector"      VARCHAR(100) NOT NULL
	, "listing_date"   DATE NOT NULL
	, "extracted_at"   TIMESTAMP NOT NULL
	, "created_at"     TIMESTAMP NOT NULL DEFAULT NOW()
	, "updated_at"     TIMESTAMP NOT NULL DEFAULT NOW()
	
);

COMMIT;


CREATE TRIGGER set_timestamp
BEFORE UPDATE ON pse.company
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

COMMIT;