from pydantic import BaseModel
from abc import ABC, abstractmethod

from pyspark.sql.session import DataFrame


class DbCredentials(BaseModel):
    """
    Class containing credentials like user, password and jdbc_url for connecting to oracle_EBS
    """
    user: str
    password: str
    jdbc_url: str

class Extractor(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def load_data_query(self, query: str):
        """
        Connects to a database and returns a PySpark DataFrame based on the provided SQL query.

        Parameters:
        - query: The SQL query to be executed against the database.

        Returns:
        - Spark DataFrame containing the results of the SQL query.
        """
        pass


    @abstractmethod
    def extract_data(self) -> DataFrame:
        pass
