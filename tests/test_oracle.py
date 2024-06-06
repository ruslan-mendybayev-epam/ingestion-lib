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
        jdbc_url = f"jdbc:oracle:thin:@//localhost:{self.oracle_container.get_exposed_port(1521)}/XE"

        db_user = "system"
        db_password = self.oracle_container.oracle_password
        db_creds = DbCredentials(db_user, db_password, jdbc_url, {"database": DB_NAME})

        query = "SELECT * FROM test_table"
        db_loader = OracleExtractor(data_contract="", spark=self.spark)
        result_df = db_loader.load_data_query(db_creds, query)

        # For geography_col, geometry_col, and hierarchyid_col are not supported out of the box for PySpark.
        assert_column_value(result_df, "id", 1)
        assert_column_value(result_df, "name", "John Doe")
        assert_column_value(result_df, "age", 30)
