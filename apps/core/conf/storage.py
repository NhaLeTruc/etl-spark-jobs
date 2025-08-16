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
        "driver": "org.postgresql:postgresql:42.7.7"
    },
    "minio-lake": {
        "container_name": "minio-lake",
        "container_port": "9000",
    }
}