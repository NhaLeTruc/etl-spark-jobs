"""
Storage facts
"""

MINIO_BUCKETS = {
    "ops": {
        "dummy": "s3://dummy",
        "lake": "s3://lake",
        "lakehouse": "s3://lakehouse",
        "dwh": "s3://warehouse",
    }
}


DOCKER_ENV = {
    "postgres": {
        "container_name": "postgres",
        "container_port": "5432",
        "user_var": "POSTGRES_USER",
        "pass_var": "POSTGRES_PASSWORD",
        "db_var": "POSTGRES_DB",
        "driver": "org.postgresql:postgresql:42.7.7"
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