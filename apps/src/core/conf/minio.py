"""
minio config objects
"""

# Externals
import os

# Internals
from core.utils import get_container_endpoint

# Variables
minio_container_name = "minio-lake"
minio_container_port = "9000"


class MinioConfig:
    """
    Abstracts supplying of necessary configuration properties for minio connections
    """

    @property
    def endpoint(self) -> str:
        """
        Hostname for minio connection
        """
        raise NotImplementedError()


    @property
    def access_key(self) -> str:
        """
        User to authenticate as for minio connection
        """
        raise NotImplementedError()
    

    @property
    def secret_key(self) -> str:
        """
        Password to authenticate as for minio connection
        """
        raise NotImplementedError()
    

class OpsMinioConfig(MinioConfig):
    """
    minio configurations specifically for connecting to OPS
    """
    def __init__(self):
        super().__init__()

    
    @property
    def endpoint(self):
        return get_container_endpoint(
            conname=minio_container_name,
            port=minio_container_port,
        )


    @property
    def access_key(self):
        return os.getenv("MINIO_ACCESS_KEY")
    

    @property
    def secret_key(self):
        return os.getenv("MINIO_SECRET_KEY")
        