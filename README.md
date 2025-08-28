# Simple Spark Application

Template for typical data engineer's local development works. Containerized local environments for data transfers / transforms / queries / visualizations. This project aims at delivering simple demonstrations of typical analytics engineering works.

## Components

1. Apache Spark & Iceberg
2. JupyterLab & Toree
3. Minio
4. Nessie Catalog
5. Apache Superset
6. Postgres & PgAdmin4

## Getting Started

Unittest runs:

```bash
# All unit tests
python3 -m unittest discover apps/test
# pipeline unit tests only
python3 -m unittest apps.test.test_pipeline
# core.utils unit tests only
python3 -m unittest apps.test.test_utils
```

Create and deploy zip files for `apps/core` and `apps/pipelines`

```bash
make apps_zip

# only `apps/core`
make core_zip

# only `apps/pipelines`
make pipe_zip
```

Docker Environment setup

```bash
# Start dev env
make up

# Shutdown dev env
make down

# Clean docker caches
make deep_clean

sudo watch docker ps -a

sudo watch docker images -a

sudo docker stop $(sudo docker ps -qa)

sudo docker start $(sudo docker ps -qa)

sudo docker rmi -f $(sudo docker images --filter=reference='data_quality_in_*' -qa)

sudo docker exec -it <CONTAINER_ID> bash

sudo docker inspect <CONTAINER_ID> | grep "IPAddress"

# Show Jupyter's kernels
jupyter kernelspec list

# Show OS info
cat /etc/os-release

# Move file from container to local
sudo docker cp <CONTAINER_ID>:</path/to/file/file.ext> </path/on/local/>

# Find Minio endpoint from spark master
curl -v minio-lake:9000 2>&1 | grep -o "(.*)." | tr -d '() '

# Load postgres example database dvdrental at https://neon.com/postgresql/postgresql-getting-started/postgresql-sample-database
scp dvdrental.zip debian@192.168.1.11:/home/debian/

unzip dvdrental.zip

pg_restore -U postgres -d dvdrental

psql -h <hostname> -p <port> -U <username> -d <database>

apt install pv

pv dvdrental.tar | pg_restore -U postgres_usr -d dvdrental
```

## Deployment

1. Ensure all required third-party Python libraries are available on the cluster or can be provided using `--py-files` or a virtual environment.
2. Package `apps/core` and `apps/pipelines` into a .zip files.
3. Configure your application to access input and output data from a location accessible by the cluster (e.g., HDFS, S3, cloud storage).
4. The spark-submit command is used to launch your PySpark application on the cluster. Key options include:
    + `--master <master_url>`: Specifies the Spark master URL (e.g., yarn, spark://master:7077, local[*]).
    + `--deploy-mode <mode>`: Determines where the driver program runs (client or cluster).
        + Client mode: The driver runs on the machine where spark-submit is executed.
        + Cluster mode: The driver runs on one of the worker nodes within the cluster.
    + `--py-files <files>`: Comma-separated list of .zip, .egg, or .py files to be added to the Python path.
    + `--driver-memory <memory>`: Amount of memory to allocate for the driver.
    + `--executor-memory <memory>`: Amount of memory to allocate for each executor.
    + `--num-executors <num>`: Number of executors to launch.
    + `<your_main_script.py>`: The path to your main PySpark application script.
    + `[arguments]`: Any arguments you want to pass to your PySpark script.

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files my_modules.zip \
  --driver-memory 2g \
  --executor-memory 4g \
  --num-executors 5 \
  my_pyspark_app.py \
  --input_path s3://my-bucket/input/data.csv \
  --output_path s3://my-bucket/output/result.csv

cp apps/pipelines/ingest_transactions/ingest_transactions.py spark/apps/

zip -r -j apps.zip apps/*

mv apps.zip spark/apps/

spark-submit --master spark://spark-master:7077 --deploy-mode client --py-files apps.zip main.py
```
