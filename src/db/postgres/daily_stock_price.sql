CREATE SCHEMA IF NOT EXISTS pse;

DROP TABLE IF EXISTS pse.daily_stock_price;

CREATE TABLE IF NOT EXISTS pse.daily_stock_price (
	  "symbol"         VARCHAR(10) NOT NULL
	, "date"           DATE NOT NULL
	, "open"           FLOAT NOT NULL
	, "high"           FLOAT NOT NULL
	, "low"            FLOAT NOT NULL
	, "close"          FLOAT NOT NULL
	, "extracted_at"   TIMESTAMP NOT NULL
	, "created_at"     TIMESTAMP NOT NULL DEFAULT NOW()
	, "updated_at"     TIMESTAMP NOT NULL DEFAULT NOW()
	
	, PRIMARY KEY ("symbol","date")
	, FOREIGN KEY ("symbol") REFERENCES pse.company ("symbol") ON DELETE CASCADE
);

COMMIT;


CREATE TRIGGER set_timestamp
BEFORE UPDATE ON pse.daily_stock_price
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

COMMIT;