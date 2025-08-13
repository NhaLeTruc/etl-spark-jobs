"""
JDBC config objects
"""

# Externals
import os

# Internals
from core.utils import get_container_endpoint

# Variables
pg_container_name = "postgres"
pg_container_port = "5432"


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
    def __init__(self):
        super().__init__()

    
    @property
    def host(self):
        return get_container_endpoint(
            conname=pg_container_name,
            port=pg_container_port,
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


    