import tempfile
import pytest
from unittest import TestCase

from delta import DeltaTable
from diskcache.core import full_name
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit, col
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType

from ingestion_lib.sinks.delta import SnapshotUnity
from ingestion_lib.utils.data_contract import DataContract


class TestDeltaDataSink(TestCase):
    @pytest.fixture(autouse=True)
    def prepare_spark_session(self, spark_session: SparkSession):
        self.spark = spark_session
        import pyspark
        print("PySpark version:", pyspark.__version__)

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory().name

        schema = StructType([
            StructField("date", StringType(), True),
            StructField("random_value1", IntegerType(), True),
            StructField("random_value2", StringType(), True),
            StructField("random_value3", StringType(), True)
        ])

        data = [
            ("2022-01-01", 1, "foo", "true"),
            ("2022-01-02", 2, "bar", "false"),
            ("2022-01-03", 3, "baz", "true"),
            ("2022-01-04", 4, "qux", "false"),
            ("2022-01-05", 5, "quux", "true"),
            ("2022-01-06", 6, "corge", "false"),
            ("2022-01-07", 7, "grault", "true"),
            ("2022-01-08", 8, "waldo", "false"),
            ("2022-01-09", 9, "fred", "true"),
            ("2022-01-10", 10, "barney", "false")
        ]

        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn("start_date", to_date(col("date"))).drop("date")
        self.data_contract = DataContract(
            batch_timestamp="2022-01-01",
            target_schema="my_schema",
            table_name="my_table",
            scope="",
            env_key=""
        )

        self.df = df
        self.df.write.format("delta").saveAsTable("my_schema.my_table")

    def test_write_snapshot_unity_delta_sink(self):
        table_full_name=f"{self.data_contract.target_schema}.{self.data_contract.table_name}"
        delta_table = DeltaTable.forName(table_full_name)
        sink = SnapshotUnity(delta_table, self.df)
        sink.write(self.data_contract)
