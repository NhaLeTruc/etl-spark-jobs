# Simple Spark Application

Template for typical data engineer's local development works. Containerized local environments for data transfers / transforms / queries / visualizations. This project aims at delivering simple demonstrations of typical analytics engineering works.

## Requirments

1. Make
2. Docker
3. Docker Compose
4. pyspark>=3.0.0
5. ruff (optional)
6. mypy (optional)

## Quickstart

>**NOTE:** the following workflow was tested on a **Debian 12 machine.**

Follow `.env.sample` template and fill sensitive information for your docker environment.

Then follow these commands to quickly setup and run the spark applications in `apps/`

```bash
# Compose docker local development environment
make up

# Wait for your environment to start and become healthy
sudo watch docker ps -a

# Populate postgres with test data
make pg_dvd

# Deploy and run pipelines on spark master
make test_pipelines

# Shutdown everything after reviewing
make down

```

Review refined dataframes and spark job execution at:

+ Minio at `[host-machine-ip]:9001` using your specified credentials in `.env`
+ PgAdmin at `[host-machine-ip]:5050` using your specified credentials in `.env`
+ spark-master at `[host-machine-ip]:8080`.
+ spark history server `[host-machine-ip]:18080`.

## Components

Local docker development environment would be setup with these components:

1. Apache Spark & Iceberg
2. JupyterLab & Toree
3. Minio
4. Nessie Catalog
5. Apache Superset
6. Postgres & PgAdmin4

## Unit test

Run unit test with:

```bash
python3 -m unittest apps.test.test_utils
# or
python3 -m unittest discover apps
```
