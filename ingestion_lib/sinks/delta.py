from dataclasses import dataclass
from typing import Union

from pyspark.sql import DataFrame

from ingestion_lib.sinks.base import DeltaDataSink
from ingestion_lib.utils.data_contract import DataContract, TableContract
from ingestion_lib.utils.databricks_utils import get_row_count_written


@dataclass
class SnapshotUnity(DeltaDataSink):
    """Class for handling snapshot writes to Delta Table."""

    def write(self, contract: Union[DataContract, TableContract], write_options: dict[str, str] = {}):
        full_table_name = f"{contract.target_schema}.{contract.table_name}"
        (self.spark_dataframe.distinct().write.format("delta").mode("overwrite").options(**write_options).saveAsTable(
            full_table_name))
        return get_row_count_written(self.spark_dataframe, table_name=full_table_name)


@dataclass
class Merge(DeltaDataSink):
    """Class for handling merge operations on Delta Table."""

    def write(self, merge_data: DataFrame):
        """Writes merge data to the Delta Table."""
        print("Merging data into Delta Table.")
        # Implement the logic to merge merge_data into self.delta_table


@dataclass
class CdcType2(DeltaDataSink):
    """Class for handling CDC Type 2 operations on Delta Table."""

    def write(self, cdc_data: DataFrame):
        """Writes CDC Type 2 data to the Delta Table."""
        print("Writing CDC Type 2 data to Delta Table.")
        # Implement the logic to write cdc_data to self.delta_table
