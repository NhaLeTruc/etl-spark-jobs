"""
JDBC config objects
"""

# Externals
import os

# Internals
from core.utils import get_container_endpoint


class JdbcConfig:
    """
    Abstracts supplying of necessary configuration properties for JDBC connections
    """

    @property
    def host(self) -> str:
        """
        Hostname for JDBC connection
        """
        raise NotImplementedError()


    @property
    def user(self) -> str:
        """
        User to authenticate as for JDBC connection
        """
        raise NotImplementedError()
    

    @property
    def password(self) -> str:
        """
        Password to authenticate as for JDBC connection
        """
        raise NotImplementedError()
        

    @property
    def dbname(self) -> str:
        """
        JDBC connection URL
        """
        raise NotImplementedError()
    

    @property
    def url(self) -> str:
        """
        JDBC connection URL
        """
        raise NotImplementedError()
    

class OpsJdbcConfig(JdbcConfig):
    """
    JDBC configurations specifically for connecting to OPS
    """
    def __init__(
        self, 
        host_name: str,
        host_port: str,
    ):
        super().__init__()
        self.host_name = host_name
        self.host_port = host_port
    
    @property
    def host(self):
        return get_container_endpoint(
            conname=self.host_name,
            port=self.host_port,
        )


    @property
    def user(self):
        return os.getenv("POSTGRES_USER")
    

    @property
    def password(self):
        return os.getenv("POSTGRES_PASSWORD")
    

    @property
    def dbname(self):
        return os.getenv("POSTGRES_DB")
    

    @property
    def url(self):
        return f"jdbc:postgresql://{self.host}/{self.dbname}"


    