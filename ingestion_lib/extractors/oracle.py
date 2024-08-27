from pyspark.sql import SparkSession

from ingestion_lib.extractors.base import Extractor
from ingestion_lib.utils.data_contract import DbCredentials, TableContract


class OracleExtractor(Extractor):

    def __init__(self, table_contract: TableContract, spark: SparkSession):
        super().__init__(table_contract, spark)

    def extract_data(self):
        return super().extract_data()

    def creds(self) -> DbCredentials:
        return self.table_contract.credentials

    def load_data_query(self, query: str):
        """
        unchecked
        :param query:
        :return:
        """
        oracle_query = f"({query}) temp"
        print(oracle_query)
        return (
            self.spark.read.format("jdbc")
            .option("url", self.creds().jdbc_url)
            .option("user", self.creds().user)
            .option("password", self.creds().password)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("dbtable", oracle_query)
            .load()
        )



