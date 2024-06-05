from pyspark.sql.session import SparkSession
from ingestion_lib.extractors.base import Extractor
from ingestion_lib.utils.data_contract import DataContract

class DbCredentials:
    """
    Class for connecting to sql server containing user, password, host and db_options(with at least database name in it)
    """

    def __init__(self, user: str, password: str, jdbc_url: str, db_options: dict[str, str]):
        self.user = user
        self.password = password
        self.jdbc_url = jdbc_url
        self.db_options = db_options

class OracleExtractor(Extractor):
    def __init__(self, data_contract: DataContract, spark: SparkSession):
        super().__init__(data_contract, spark)
    def extract_data(self):
        # Oracle-specific extraction logic
        pass

    def creds(self):
        pass

    def load_data_query(self, creds:DbCredentials, query: str):
        """
        unchecked
        :param query:
        :return:
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", creds.jdbc_url)
            .option("user", creds.user)
            .option("password", creds.password)
            .option("database", creds.db_options["database"])
            .option("query", query)
            .load()
        )



