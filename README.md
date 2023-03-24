pse-stocks-etl
==============

ETL jobs for syncing PSE stocks data (PSE Edge) to a Postgres database.

---


System Requirements
---------------------

1. Mac or Linux machine
1. Miniconda
1. Postgres or Postgres-compatible database


Setup
-------

### Clone project repo

Clone the project inside `$HOME`, then go inside the project folder.

```sh
cd $HOME;
git clone https://github.com/anthonym8/pse-stocks-etl.git;
cd pse-stocks-etl
```

### Install python and packages

Create a new python environment, then activate this environment:

```sh
conda create -n pse-stocks-etl python==3.8;
conda activate pse-stocks-etl;
```

Install python packages:

```sh
pip install -r requirements.txt;
```

### Set up database credentials

Copy template file as `.env`.

```sh
cp sample.env .env
```

Input PostgreSQL database endpoint and credentials.

```sh
...
DATABASE_ENDPOINT=
DATABASE_USERNAME=
DATABASE_PASSWORD=
DATABASE_PORT=
DATABASE_NAME=
```

### Deploy script

Open crontab:

```sh
crontab -e
```

Create a cron job as follows:

```sh
0 16 * * * cd $HOME/pse-stocks-etl; $HOME/miniconda3/envs/pse-stocks-etl/bin/python -m src.etl.sync;
```

---


Usage
-----

**Manual backfill:**

```sh
cd $HOME/pse-stocks-etl;
conda activate pse-stocks-etl;
python -m src.etl.backfill;
```

**Manual sync:**

```sh
cd $HOME/pse-stocks-etl;
conda activate pse-stocks-etl;
python -m src.etl.sync;

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
