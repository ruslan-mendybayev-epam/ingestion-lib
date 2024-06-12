from pyspark.sql.functions import lit
from ingestion_lib.extractors.base import Extractor
from pyspark.sql import DataFrame, SparkSession

from ingestion_lib.extractors.oracle import OracleExtractor
from ingestion_lib.utils.data_contract import TableContract
from ingestion_lib.utils.databricks_utils import get_row_count_written, get_delta_write_options


class DatabricksIngestionProvider:
    def __init__(self, extractor: Extractor):
        self.extractor = extractor

    @staticmethod
    def init_oracle_with_table_contract(table_contract: TableContract, spark: SparkSession):
        # TODO: create better logic to handle Extractor Creation
        return DatabricksIngestionProvider(OracleExtractor(table_contract, spark))

    def normalize_data(self, data) -> DataFrame:
        for column in data.columns:
            data = data.withColumnRenamed(column, column.replace(" ", "_"))
        return data

    def write_data(self, data, target_schema, mount_point, table_name, options):
        # Logic to write data to Databricks Delta Lake
        location = f"{mount_point}/{target_schema}/{table_name}"
        (data.distinct().write.format("delta").mode("overwrite").options(**options).save(location))
        return get_row_count_written(data, location=location)

    def execute_ingestion(self):
        """
        Missing steps:
        **Step 9: Run data quality checks (optional)**
        Description: Run data quality checks on the DataFrame if a contract is provided.
        Details: If a contract is provided and it contains "dq_rules", the `DataQualityChecker` class is used to run data quality checks.

        **Step 11: Log row count and return**
        Description: Log the row count of the written DataFrame and return.
        Details: The row count is logged, and the method returns the row count.
        :return:
        """
        table_contract = self.extractor.table_contract
        data = self.extractor.extract_data()
        normalized_data = self.__add_timestamp_column(table_contract.batch_timestamp, self.normalize_data(data))

        result = self.write_data(normalized_data, table_contract.target_schema, table_contract.mount_point,
                                 table_contract.table_name, options=get_delta_write_options(table_contract))

    def __add_timestamp_column(self, batch_timestamp: str, df: DataFrame) -> DataFrame:
        """
        Adds timestamp column to the input DataFrame.

        Parameters:
        - load_timestamp: The timestamp to be added to the DataFrame.
        - df: The Spark DataFrame to which the timestamp column will be added.

        Returns:
        - The Spark DataFrame with a new column named 'load_timestamp' that contains the provided timestamp.
        """
        return df.withColumn("load_timestamp", lit(batch_timestamp).cast("timestamp"))
