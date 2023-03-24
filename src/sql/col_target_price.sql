CREATE SCHEMA IF NOT EXISTS col;

DROP TABLE IF EXISTS col.target_price;

CREATE TABLE IF NOT EXISTS col.target_price (
	  "symbol"           VARCHAR(10) NOT NULL
	, "future_price"     FLOAT NOT NULL
	, "buy_below_price"  FLOAT NOT NULL
	, "created_at"       TIMESTAMP NOT NULL DEFAULT NOW()
	, "updated_at"       TIMESTAMP NOT NULL DEFAULT NOW()
	
	, PRIMARY KEY ("symbol")
	, FOREIGN KEY ("symbol") REFERENCES pse.company ("symbol")
);

COMMIT;


CREATE TRIGGER set_timestamp
BEFORE UPDATE ON col.target_price
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

COMMIT;