from ingestion_lib.extractors.base import Extractor


class OracleExtractor(Extractor):
    def extract_data(self):
        # Oracle-specific extraction logic
        pass

    def creds(self):
        pass

    def load_data_query(self, query: str):
        """
        unchecked
        :param query:
        :return:
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", self.creds.jdbc_url)
            .option("user", self.creds.user)
            .option("password", self.creds.password)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("dbtable", query)
            .load()
        )



