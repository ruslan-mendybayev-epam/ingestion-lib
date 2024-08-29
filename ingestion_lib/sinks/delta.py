from dataclasses import dataclass

from pyspark.sql import DataFrame

from ingestion_lib.sinks.base import DeltaDataSink


@dataclass
class Snapshot(DeltaDataSink):
    """Class for handling snapshot writes to Delta Table."""

    def write(self, snapshot_data: DataFrame):
        """Writes snapshot data to the Delta Table."""
        print("Writing snapshot data to Delta Table.")
        # Implement the logic to write snapshot_data to self.delta_table


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
