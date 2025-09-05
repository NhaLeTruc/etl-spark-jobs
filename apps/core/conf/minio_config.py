"""
minio config objects
"""

import os


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


class DockerEnvMinioConfig(MinioConfig):
    """
    configurations specifically for connecting to OPS's minio-lake
    """
    def __init__(
        self,
        config: dict = {}
    ):
        super().__init__()
        self.config = config


    @property
    def endpoint(self):
        command = "curl -v " + self.config.get('container_name') + ":" + self.config.get('container_port') + " 2>&1 | grep -o -m 1 '(.*).' | tr -d '() '"
        return "http://" + os.popen(command).read().replace('\n', '') + ":" + self.config.get("container_port")


    @property
    def access_key(self):
        return os.getenv(self.config.get("user_var"))


    @property
    def secret_key(self):
        return os.getenv(self.config.get("pass_var"))
