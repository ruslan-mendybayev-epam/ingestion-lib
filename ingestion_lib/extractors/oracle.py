from pyspark.sql.session import SparkSession, DataFrame

from ingestion_lib.extractors.base import Extractor, DbCredentials
from ingestion_lib.utils.data_contract import TableContract


class OracleExtractor(Extractor):
    """
    A data extractor for Oracle databases.

    This class is designed to facilitate the extraction of data from Oracle databases using Spark. It initializes
    an instance with the necessary details to connect to the database and perform data extraction tasks.
    
    Parameters:
    - table_contract (TableContract): The contract for the table from which data will be extracted.
    - spark (SparkSession): The Spark session to be used for data extraction operations.
    - creds (DbCredentials): The database credentials required for connecting to the Oracle database.
    """

    def __init__(self, table_contract: TableContract, spark: SparkSession, creds: DbCredentials):
        self.table_contract = table_contract
        self.spark = spark
        self.creds = creds
        self.table = self.table_contract.table_name
        self.schema = self.table_contract.schema_name
        self.watermark_columns = self.table_contract.watermark_columns

    def load_data_query(self, query: str):
        """
        This method establishes a connection to a database using JDBC URL, credentials, Oracle JDBC driver and a specified SQL query.
        It reads the data into a Spark DataFrame.

        Parameters:
        - query (str): The SQL query string used to select data from the database.

        Returns:
        - DataFrame: A Spark DataFrame containing the data retrieved from the database based on the input query.
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", self.creds.jdbc_url)
            .option("dbtable", query)
            .option("user", self.creds.user)
            .option("password", self.creds.password)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .load()
        )
    
    def load_data_query_partitioned(self, query: str, watermark_column: str):
        """
        This method establishes a connection to a database using JDBC URL, credentials, Oracle JDBC driver and a specified SQL query.
        It reads the data into a Spark DataFrame which is partitioned on the basis of jdbc config.

        Parameters:
        - query (str): The SQL query string used to select data from the database.
        - watermark_column (str): Modified date (timestamp) column

        Returns:
        - DataFrame: A Spark DataFrame containing the data retrieved from the database based on the input query.
        """
        min_max_query = f"(select min({watermark_column}), max({watermark_column}) from ({query})) temp"
        min_max_df = self.load_data_query(min_max_query)
        lower_bound = min_max_df.collect()[0][0].strftime('%Y%m%d%H%M%S')
        upper_bound = min_max_df.collect()[0][1].strftime('%Y%m%d%H%M%S')
        timestamp_query = f"{self.table}.*, To_NUMBER(TO_CHAR({watermark_column}, 'YYYYMMDDHH24MISS')) timestamp_num"
        updated_query = query.replace("*", timestamp_query)


        return (
            self.spark.read.format("jdbc")
            .option("url", self.creds.jdbc_url)
            .option("dbtable", updated_query)
            .option("user", self.creds.user)
            .option("password", self.creds.password)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("partitionColumn", "timestamp_num")
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
            .option("numPartitions", "32")
            .load()
        )

    def extract_data(self) -> DataFrame:
            """
            Extracts data from an Oracle database based on the table contract and conditions.

            Returns:
            - DataFrame: A Spark DataFrame containing the data retrieved from the database based on the input query.
            """
            
            # TODO: Add invalid type checks
            select_query = self.__build_select_query()
            condition = self.__build_condition()
            query = f"({select_query}{condition}) temp"
            print(f"ingestion query: {query}")
            if not self.watermark_columns:           
                data = self.load_data_query(query)
            elif len(self.watermark_columns) == 1:
                data = self.load_data_query_partitioned(query, self.watermark_columns[0]).drop("timestamp_num")
            # TODO: Add logging at debug level
            if self.watermark_columns and len(self.watermark_columns) > 1:
                data = self.load_data_query_partitioned(query, "watermark_column_")
                # dropping column 'watermark_column' which is having max timestamp if there are multiple timestamp columns
                data = data.drop("watermark_column_", "timestamp_num")
            return data

    def __build_condition(self) -> str:
        """
        **Step 5: Build condition (if watermark columns are present)**
        Description: If watermark columns are present in the `self.table_contract` object, the method builds a condition clause using the `__build_condition` method.
        Details: The condition clause is constructed based on the watermark columns, and is used to filter the data retrieved from the SQL Server database.
        :return:
        """

        if not self.watermark_columns or self.table_contract.full_load == True or self.table_contract.load_type == "one_time":
            return ""
        elif len(self.watermark_columns) == 1:
            return (
                    f""" WHERE {self.watermark_columns[0]} >= TO_DATE('{self.table_contract.lower_bound}', 'YYYY-MM-DD"T"HH24:MI:SS') """
                    + f"""AND {self.watermark_columns[0]} < TO_DATE('{self.table_contract.upper_bound}', 'YYYY-MM-DD"T"HH24:MI:SS')"""
            )
        else:
            return (
                    f" WHERE watermark_column_ >= '{self.table_contract.lower_bound}' " + f"AND watermark_column_ < '{self.table_contract.upper_bound}'"
            )

    def __build_select_query(self) -> str:
        """
        **Step 4: Build select query (if no invalid types)**
        Description: If no invalid types are present, the method builds a select query using the `__build_select_query` method.
        Details: The select query is constructed based on the table and schema information, and is used to retrieve data from the SQL Server database.

        :return:
        """
        if not self.watermark_columns or self.table_contract.full_load == True or len(self.watermark_columns) == 1:
            return f"SELECT * FROM {self.schema}.{self.table}"
        else:
            watermark_columns = ", ".join([f"({col})" for col in self.watermark_columns])
            return f"""
                    SELECT *
                    FROM (
                        SELECT 
                        *, 
                        (SELECT MAX(watermark_column_)
                            FROM (VALUES {watermark_columns}) AS last_updated(watermark_column_)) 
                        AS watermark_column_
                    
                    """
