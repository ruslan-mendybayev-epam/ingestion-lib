from ingestion_lib.extractors.base import Extractor
from pyspark.sql import DataFrame, SparkSession

from ingestion_lib.extractors.oracle import OracleExtractor
from ingestion_lib.utils.data_contract import DataContract
from ingestion_lib.utils.databricks_utils import get_row_count_written, get_delta_write_options


class DatabricksIngestionProvider:
    def __init__(self, extractor: Extractor):
        self.extractor = extractor

    @staticmethod
    def init_oracle_with_data_contract(data_contract: DataContract, spark: SparkSession):
        # TODO: create better logic to handle Extractor Creation
        return DatabricksIngestionProvider(OracleExtractor(data_contract, spark))

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
        **Step 4: Check if table exists (one-time load only)**
        Description: Check if the table exists in the "bronze" layer.
        Details: If the `load_type` is "one_time" and the table exists, the method returns 0, skipping the ingestion process.

        **Step 7: Add timestamp column**
        Description: Add a "load_timestamp" column to the DataFrame.
        Details: The `add_timestamp_column()` method is called to add the timestamp column.

        **Step 8: Create table if not exists**
        Description: Create the table in the "bronze" layer if it does not exist.
        Details: The `create_table_if_not_exist()` function is called to create the table.

        **Step 9: Run data quality checks (optional)**
        Description: Run data quality checks on the DataFrame if a contract is provided.
        Details: If a contract is provided and it contains "dq_rules", the `DataQualityChecker` class is used to run data quality checks.

        **Step 11: Log row count and return**
        Description: Log the row count of the written DataFrame and return.
        Details: The row count is logged, and the method returns the row count.
        :return:
        """
        data_contract = self.extractor.data_contract
        data = self.extractor.extract_data()
        normalized_data = self.normalize_data(data)
        result = self.write_data(normalized_data, data_contract.target_schema, data_contract.mount_point,
                                 data_contract.table_name, options=get_delta_write_options(data_contract))
