pse-stocks-etl
==============

[![Build](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/build.yml/badge.svg)](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/build.yml)
[![Tests](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/test.yml/badge.svg)](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/test.yml)
[![Daily Sync Job](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/daily-sync-job.yml/badge.svg)](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/daily-sync-job.yml)

Data pipeline for syncing stock price data from PSE Edge (Philippine Stock Exchange) to a Postgres database.

---

Technical Overview
------------------

This project demonstrates a functional data pipeline where data is **extracted** from REST APIs (PSE Edge), 
cleaned and **transformed** via custom python functions, and **loaded** to a destination database (Postgres)
for analytics consumption.

The pipeline populates the following database tables:
1. `pse.company` → Contains all PSE-listed companies including relevant attributes e.g. company name, ticker symbol, sector, etc.
1. `pse.daily_stock_price` → Contains daily stock price data (open, high, low, close) per PSE-listed company.

---

Usage and Deployment
--------------------

This ETL pipeline is packaged and deployed as a docker image. For scheduled runs, you can:
- Use cron to run the container on Amazon EC2 on a schedule.
- Deploy on a container orchestration service like Amazon ECS or Google Cloud Run.
- Use Github Actions to run the sync job on a schedule 🤩 (like [this](https://github.com/anthonym8/pse-stocks-etl/actions/workflows/daily-sync-job.yml)).

### Building the Docker image

Clone this repository.
```
git clone git@github.com:anthonym8/pse-stocks-etl.git`
`cd pse-stocks-etl
```

Build the image.
```
docker build --tag pse-stocks-etl .
```

### Running the ETL job

Use `docker run` to create a container and execute the sync job. Provide the database credentials as follows:
```
docker run \
    -e DATABASE_ENDPOINT={{ db-host }} \
    -e DATABASE_USERNAME={{ username }} \
    -e DATABASE_PASSWORD={{ password }} \
    -e DATABASE_PORT={{ port }} \
    -e DATABASE_NAME={{ db-name }} \
    pse-stocks-etl
```

---

Development
-----------
For development purposes, it is recommended to have the following setup:

1. Mac or Linux machine
1. Miniconda
1. Postgres or Postgres-compatible database

### Environment Setup

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

Set up database credentials

Copy template file as `.env`.

```sh
cp sample.env .env
```

Input PostgreSQL database endpoint and credentials with sufficient privileges.

```sh
...
DATABASE_ENDPOINT=
DATABASE_USERNAME=
DATABASE_PASSWORD=
DATABASE_PORT=
DATABASE_NAME=
```

Initialize the database tables.

```
python -m src.db.init
```

---


Project Organization
--------------------

```
├── README.md            <- The top-level README for developers using this project.
├── requirements.txt     <- The requirements file for reproducing the python environment.
├── sample.env           <- Template for the .env file where DB credentials will be stored.
│
├── src                  <- Contains source code files
│   ├── etl
│   │   ├── backfill.py  <- Python script to backfill historical data completely.
│   │   └── sync.py      <- Main python script or syncing data from source to destination.
│   │
│   ├── sql              <- DDL statements for the destination tables in PostgreSQL.                 
│   └── utils            <- Utility and helper functions
│
└── tests                <- Test scripts.                 
```
