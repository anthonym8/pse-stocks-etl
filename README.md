pse-stocks-etl
==============

[![Build](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/build.yml/badge.svg)](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/build.yml)
[![Tests](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/tests.yml/badge.svg)](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/tests.yml)
[![Daily Sync Job](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/daily-sync-job.yml/badge.svg)](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/daily-sync-job.yml)

Data pipeline for syncing stock price data from PSE Edge (Philippine Stock Exchange) to various destination databases.

---

Overview
------------------

This project demonstrates a functional data pipeline where data is **extracted** from REST APIs (PSE Edge), 
cleaned and **transformed** via custom python functions, and **loaded** to a destination database (e.g. Postgres, BigQuery, etc.)
for analytics consumption.

The pipeline populates the following database tables:
1. `pse.company` → Contains all PSE-listed companies including relevant attributes e.g. company name, ticker symbol, sector, etc.
1. `pse.daily_stock_price` → Contains daily stock price data (open, high, low, close) per PSE-listed company.

Pipeline destination include the following storage options:
1. Postgres
1. BigQuery
1. Delta Lake on Google Cloud Storage (via [delta-rs](https://delta-io.github.io/delta-rs/python/))
1. Delta Lake on Google Cloud Storage (via Apache Spark)

---

Development
--------------------

For development purposes, it is recommended to have the following setup:

1. Mac or Linux machine
1. Miniconda
1. Postgres or Postgres-compatible database
1. Google Cloud account with creator access to Google Cloud Storage and BigQuery

#### Environment Setup

Clone this repository.

```sh
git clone https://github.com/anthonym8/pse-stocks-etl.git;
cd pse-stocks-etl
```

Install python and packages.

```sh
conda create -n pse-stocks-etl python==3.8;
conda activate pse-stocks-etl;
pip install -r requirements.txt;
```

#### Postgres Database Setup

Set up database credentials. Copy template file as `.env`.

```sh
cp sample.env .env
```

Input PostgreSQL database endpoint and credentials with sufficient privileges.

```sh
...
POSTGRES_DB_ENDPOINT=
POSTGRES_DB_USERNAME=
POSTGRES_DB_PASSWORD=
POSTGRES_DB_PORT=
POSTGRES_DB_NAME=
```

Initialize the database tables.

```
python -m src.main --destination postgres --action initdb
```

#### BigQuery Database Setup

Create a JSON keyfile from Google Cloud Console 
then save it as `keyfile.json` inside the `credentials` folder.

Set up database credentials. Copy template file as `.env`.

```sh
cp sample.env .env
```

Input PostgreSQL database endpoint and credentials with sufficient privileges.

```sh
...
GCP_PROJECT_ID=
GCP_CREDENTIALS_FILE=
GCS_BUCKET_NAME=
BIGQUERY_LOCATION=
BIGQUERY_DATASET_ID=
```

Initialize the database tables.

```
python -m src.main --destination bigquery --action initdb
```

#### Delta Lake Setup

Create a JSON keyfile from Google Cloud Console 
then save it as `keyfile.json` inside the `credentials` folder.

Set up Google Cloud credentials. Copy template file as `.env`.

```sh
cp sample.env .env
```

Provide values to the following variables: 

```sh
...
GCP_PROJECT_ID=
GCP_CREDENTIALS_FILE=
DELTA_TABLE_PATH_PREFIX=

```


### Local Spark Runtime

For development purposes, you may build a single-node Spark cluster provided in this repo.

```sh
docker build --tag pse-stocks-etl-spark-dev --file src/db/delta_spark/Dockerfile-spark-dev .
```

Run JupyterLab using the dev container while mounting the relevant python scripts:

```sh
docker run -it \
	-v src/etl/spark_deltalake_sync.py:/home/glue_user/pse-stocks-etl/spark_deltalake_sync.py \
	-v src/utils/multithreading.py:/home/glue_user/pse-stocks-etl/multithreading.py \
	-v src/utils/pse_edge.py:/home/glue_user/pse-stocks-etl/pse_edge.py \
	-v .env:/home/glue_user/pse-stocks-etl/.env \
	-v credentials:/home/glue_user/pse-stocks-etl/credentials \
	-v data:/home/glue_user/pse-stocks-etl/data \
	-e DISABLE_SSL=true \
	-e DATALAKE_FORMATS=delta \
	-p 18080:18080 \
	-p 8998:8998 \
	-p 4040:4040 \
	--rm \
	--name pse-stocks-spark pse-stocks-etl-spark-dev \
	/home/glue_user/jupyter/jupyter_start.sh
```

Execute spark job via `spark-submit` using the dev container:

```sh
docker run -it \
	-v src/etl/spark_deltalake_sync.py:/home/glue_user/pse-stocks-etl/spark_deltalake_sync.py \
	-v src/utils/multithreading.py:/home/glue_user/pse-stocks-etl/multithreading.py \
	-v src/utils/pse_edge.py:/home/glue_user/pse-stocks-etl/pse_edge.py \
	-v .env:/home/glue_user/pse-stocks-etl/.env \
	-v credentials:/home/glue_user/pse-stocks-etl/credentials \
	-v data:/home/glue_user/pse-stocks-etl/data \
	-e DISABLE_SSL=true \
	-e DATALAKE_FORMATS=delta \
	-p 18080:18080 \
	-p 8998:8998 \
	-p 4040:4040 \
	--rm \
	--name pse-stocks-spark pse-stocks-etl-spark \
	spark-submit --py-files multithreading.py,pse_edge.py spark_deltalake_sync.py
```


#### Testing

Run unit tests:

```sh
pytest tests/unit
```

Run integration tests:

```sh
pytest tests/integration
```

End-to-end pipeline test:

> See Usage and Deployment > Running locally.


---

Usage and Deployment
--------------------

This ETL pipeline is packaged and deployed as a docker image. For scheduled runs, you can:
- Use cron to run the container on Amazon EC2 on a schedule.
- Deploy on a container orchestration service like Amazon ECS or Google Cloud Run.
- Use Github Actions to run the sync job on a schedule 🤩 (like [this](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/daily-sync-job.yml)).

#### Running locally

Run the data pipeline.

```
python -m src.main --destination postgres --action sync
```


#### Running via Docker

Build the image.
```
docker build --tag pse-stocks-etl .
```

Run the data pipeline.

```
docker run \
    -e POSTGRES_DB_ENDPOINT={{ db-host }} \
    -e POSTGRES_DB_USERNAME={{ username }} \
    -e POSTGRES_DB_PASSWORD={{ password }} \
    -e POSTGRES_DB_PORT={{ port }} \
    -e POSTGRES_DB_NAME={{ db-name }} \
    pse-stocks-etl --destination postgres --action sync
```


### Running via Spark

Use this command to run the spark job:

```
spark-submit --py-files multithreading.py,pse_edge.py spark_deltalake_sync.py
```

Depending on your spark runtime, you may need to configure additional dependencies:
- Additional jars:
    - `gcs-connector-hadoop3-latest.jar`
    - `delta-core_2.12-2.1.0.jar`
    - `delta-storage-2.1.0.jar`
- Additional python libraries:
    - `delta-spark`
    - others (see `src/db/delta_spark/requirements.txt`)

---


Project Organization
--------------------

```
├── README.md            <- The top-level README for developers using this project.
├── requirements.txt     <- The requirements file for reproducing the python environment.
├── sample.env           <- Template for the .env file where DB credentials will be stored.
│
├── src                  <- Contains source code files
│   ├── main.py          <- Main ETL script
│   ├── db               <- Database init script + DDL statements for the destination tables.                 
│   ├── etl              <- ETL functions for various destination databases.
│   └── utils            <- Utility and helper functions
│
└── tests                <- Test scripts          
```
