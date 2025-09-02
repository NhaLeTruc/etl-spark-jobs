"""
Storage facts
"""

MINIO_BUCKETS = {
    "ops": {
        "dummy": "s3a://dummy",
        "lake": "s3a://lake",
        "lakehouse": "s3a://lakehouse",
        "dwh": "s3a://warehouse",
    }
}


DOCKER_ENV = {
    "postgres": {
        "container_name": "postgres",
        "container_port": "5432",
        "user_var": "POSTGRES_USER",
        "pass_var": "POSTGRES_PASSWORD",
        "db_var": "dvdrental",
        "driver": "org.postgresql.Driver"
    },
    "minio-lake": {
        "container_name": "minio-lake",
        "container_port": "9000",
        "user_var": "MINIO_ACCESS_KEY",
        "pass_var": "MINIO_SECRET_KEY",
        "db_var": "",
        "driver": ""
    }
}


OPS_SCHEMAS = "dvdrental.public"
bucket_lake = MINIO_BUCKETS["ops"]["lake"] + "/OPS/rental_bronze"
bucket_lakehouse = MINIO_BUCKETS["ops"]["dwh"] + "/OPS/rental_silver"
bucket_house = MINIO_BUCKETS["ops"]["lakehouse"] + "/OPS/rental_gold"