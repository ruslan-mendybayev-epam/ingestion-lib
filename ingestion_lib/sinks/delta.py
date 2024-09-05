from dataclasses import dataclass
from typing import Union

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import lit, sha2, concat_ws, row_number, col, current_timestamp

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
    condition = ['Hash', 'CurrentFlag', 'DeletedFlag']

    def write(self, cdc_data: DataFrame):
        # 1 check if delta table has condition columns

        # 2 Find rows_to_update
        source_df = self.spark_dataframe
        target_df = self.delta_table.toDF()
        condition = self.condition  # TODO add extra condition keys from contract
        columns = source_df.columns
        RowsToUpdate = source_df \
            .alias("source") \
            .where("CurrentFlag = 'Y' AND DeletedFlag = 'N'") \
            .join(target_df.alias("target"), condition, 'leftanti') \
            .select(*columns) \
            .orderBy(col('source.DimId'))  # TODO put extra condition keys from contract

        # 3 find max surrogate key and add to Update rows
        maxTableKey = target_df.agg({"SurrogateKey": "max"}).collect()[0][0]
        RowsToUpdate = RowsToUpdate.withColumn("SurrogateKey", col("SurrogateKey") + maxTableKey)

        # Merge statement to expire old records
        self.delta_table.alias("original").merge(
            source=RowsToUpdate.alias("updates"),
            condition='original.DimId = updates.DimId' # TODO put extra conditions keys
        ).whenMatchedUpdate(
            condition="original.CurrentFlag = 'Y' AND original.DeletedFlag = 'N' AND original.Hash <> updates.Hash",
            set={
                "CurrentFlag": "'N'",
                "EffectiveToDate": lit(current_timestamp()) # TODO put batch timestamp here
            }
        ).execute()
        # Insert all new and updated records
        full_table_name = f"" # TODO put contract table name here
        RowsToUpdate.select(*columns).write.mode("Append").format("delta").saveAsTable(full_table_name)
        # Handle deletes
        RowsToDelete = gold.alias('gold').where("CurrentFlag = 'Y' AND DeletedFlag = 'N'") \
            .join(silver.alias('silver'), col('silver.DimId') == col('gold.DimId'), "leftanti")
        # Merge statement to mark as deleted records
        DeltaTable.forPath(spark, gold_path).alias("original").merge(
            source = RowsToDelete.alias("deletes"),
            condition = 'original.DimId = deletes.DimId'
        ).whenMatchedUpdate(
            condition = "original.CurrentFlag = 'Y' AND original.DeletedFlag = 'N'",
            set = {
                "DeletedFlag": "'Y'",
                "EffectiveToDate": lit(current_timestamp())
            }
        ).execute()
        ...
