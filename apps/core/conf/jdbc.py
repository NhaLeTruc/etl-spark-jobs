"""
JDBC config objects
"""

# Externals
import os
from typing import Dict


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
    
    @property
    def driver(self) -> str:
        """
        JDBC driver
        """
        raise NotImplementedError()
    

# TODO: implement vault for non-prod env
class DockerEnvJdbcConfig(JdbcConfig):
    """
    JDBC configurations specifically for connecting to OPS
    """
    def __init__(
        self, 
        config: Dict={}
    ):
        super().__init__()
        self.config = config
    
    @property
    def host(self):
        CMD = f"curl -v {self.config.get("container_name")}:{self.config.get("container_name")} 2>&1 | grep -o '(.*).' | tr -d '() '"
        return "http://" + os.popen(CMD).read().replace('\n', '') + ":" + {self.config.get("container_port")}


    @property
    def user(self):
        return os.getenv(self.config.get("user_var"))
    

    @property
    def password(self):
        return os.getenv(self.config.get("pass_var"))
    

    @property
    def dbname(self):
        return os.getenv(self.config.get("db_var"))
    

    @property
    def url(self):
        return f"jdbc:postgresql://{self.host}/{self.dbname}"
    

    @property
    def driver(self) -> str:
        return self.config.get("driver")


    