"""
minio config objects
"""

# Externals
import os

# Internals
from core.utils import get_container_endpoint


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
    configurations specifically for connecting to OPS's minio-lake
    """
    def __init__(
        self,
        minio_host: str,
        minio_port: str,
    ):
        super().__init__()
        self.minio_host = minio_host
        self.minio_port = minio_port

    
    @property
    def endpoint(self):
        return get_container_endpoint(
            conname=self.minio_host,
            port=self.minio_port,
        )


    @property
    def access_key(self):
        return os.getenv("MINIO_ACCESS_KEY")
    

    @property
    def secret_key(self):
        return os.getenv("MINIO_SECRET_KEY")
        