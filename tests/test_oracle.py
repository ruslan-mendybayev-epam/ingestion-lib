import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

from testcontainers.oracle import OracleDbContainer

from unittest import mock
from unittest.mock import patch, Mock

import pytest
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from math import isclose

from tests.db_test_settings import DB_NAME
from tests.db_testing_utils import assert_column_value

from ingestion_lib.extractors.oracle import DbCredentials, OracleExtractor

class TestOracleLoader(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def prepare_spark_session(self, spark_session: SparkSession):
        self.spark = spark_session

    @pytest.fixture(autouse=True)
    def prepare_oracle_container(self, oracle_container: OracleDbContainer):
        self.oracle_container = oracle_container

    # @pytest.fixture(autouse=True)
    # def prepare_database_connection(self, database_connection: DatabaseConnection):
    #     self.database_connection = database_connection

    @classmethod
    def setUp(self):
        self.schema = "edw"

    @classmethod
    def tearDown(self):
        pass  # You can do any cleanup here, if needed

    UPPER_BOUND = "2"
    LOWER_BOUND = "1"

    DB = "db"
    TABLE = "table"
    SCHEMA = "schema"
    GEOGRAPHY_COL = "geography_col"
    GEOGRAPHY = "geography"
    COLUMN_NAME = "column_name"
    DATA_TYPE = "data_type"

    def create_default_values_df(self):
        schema = StructType([StructField("id", StringType(), True), StructField("name", StringType(), True)])
        return self.spark.createDataFrame([("12", "smith")], schema=schema)

    def create_empty_metadata_df(self):
        return self.spark.createDataFrame([], StructType([]))

    def create_metadata_df(self):
        schema = StructType([StructField(self.DATA_TYPE, StringType(), True), StructField(self.COLUMN_NAME, StringType(), True)])
        return self.spark.createDataFrame([(self.GEOGRAPHY, self.GEOGRAPHY_COL)], schema=schema)

    def execute_script(self, script_path):
        self.sqlserver_container.exec

    # @patch("pyspark.sql.SparkSession")
    def test_load_data_query(self):
        jdbc_url = f"jdbc:oracle:thin:@//localhost:{self.oracle_container.get_exposed_port(1521)}/FREEPDB1"

        db_user = "system"
        db_password = self.oracle_container.oracle_password
        db_creds = DbCredentials(db_user, db_password, jdbc_url, {"database": DB_NAME})

        query = "SELECT * FROM DATA_TYPES_TABLE"
        db_loader = OracleExtractor(data_contract="", spark=self.spark)
        result_df = db_loader.load_data_query(db_creds, query)

        # For geography_col, geometry_col, and hierarchyid_col are not supported out of the box for PySpark.
        assert_column_value(result_df, "binary_col", bytes([0x15]))
        assert_column_value(result_df, "varbinary_col", bytearray([0x15]))
        assert_column_value(result_df, "char_col", "a")
        assert_column_value(result_df, "varchar_col", "a")
        assert_column_value(result_df, "nchar_col", "a")
        assert_column_value(result_df, "nvarchar_col", "a")
        assert_column_value(result_df, "datetime_col", datetime.strptime("2020-12-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
        assert_column_value(result_df, "smalldatetime_col", datetime.strptime("2020-12-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
        assert_column_value(result_df, "date_col", datetime.strptime("2020-12-01", "%Y-%m-%d").date())
        assert_column_value(result_df, "time_col", datetime.strptime("1970-01-01 10:34:23", "%Y-%m-%d %H:%M:%S"))
        assert_column_value(result_df, "datetimeoffset_col", "2025-12-10 12:32:10.0000000 +01:00")
        assert_column_value(result_df, "datetime2_col", datetime.strptime("2016-12-21 00:00:00", "%Y-%m-%d %H:%M:%S"))
        assert_column_value(result_df, "decimal_col", Decimal("476.29"))
        assert_column_value(result_df, "numeric_col", Decimal("12345.12"))
        assert_column_value(result_df, "float_col", float("1245.12"))
        assert_column_value(result_df, "bigint_col", 15)
        assert_column_value(result_df, "int_col", 15)
        assert_column_value(result_df, "smallint_col", 15)
        assert_column_value(result_df, "tinyint_col", 1)
        assert_column_value(result_df, "bit_col", True)
        assert_column_value(
            result_df,
            "image_col",
            bytearray(b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00H\x00H\x00\x00\xff\xed\x00;Adobe\x00d\x00\x00\x00"),
        )
        assert_column_value(result_df, "ntext_col", "testing 1,2,3")
        assert_column_value(result_df, "text_col", "text")
        assert_column_value(result_df, "xml_col", '<employee><firstname type="textbox">Jimmy</firstname></employee>')
        assert_column_value(result_df, "custom_varchar20_col", "custom varchar type")

        expected_value = Decimal("3148.29").quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        assert_column_value(result_df, "money_col", expected_value)
        assert_column_value(result_df, "smallmoney_col", expected_value)

        # For uniqueidentifier_col, you can only check if the value is not null, as NEWID() generates UUID dynamically.
        assert result_df.filter(col("uniqueidentifier_col").isNull()).count() == 0

        assert isclose(result_df.first()["real_col"], 96.602, rel_tol=1e-5), "Expected '96.602' in column 'real_col'."
