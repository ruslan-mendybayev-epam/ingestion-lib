from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import DataFrame
from delta.tables import DeltaTable


@dataclass
class DataSink(ABC):
    delta_table: DeltaTable
    spark_dataframe: DataFrame

    @abstractmethod
    def write(self, *args, **kwargs):
        """Abstract method to write data. Must be implemented by child classes."""
        pass

    def dq(self):
        """Hook method for data quality checks. Can be overridden by child classes."""
        print("Performing data quality checks.")


@dataclass
class DeltaDataSink(DataSink):
    """Class for handling Delta Table data sinks."""

    @abstractmethod
    def write(self, *args, **kwargs):
        """Abstract method to write data to Delta Table. Must be implemented by child classes."""
        pass
