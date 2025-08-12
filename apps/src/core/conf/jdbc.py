"""
JDBC config objects
"""

# Externals
import os

# Internals


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
        return self.get_postgres_host()


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
        return f"jdbc:postgresql://{self.get_postgres_host()}/{self.dbname}"


    def get_postgres_host() -> str:
        """
        Get url address of postgres container in dev docker network
        """
        CMD = "curl -v postgres:5432 2>&1 | grep -o '(.*).' | tr -d '() '"
        return 'http://' + os.popen(CMD).read().replace('\n', '')  + ':5432'
