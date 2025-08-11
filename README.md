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
```
