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
            data = self.load_data_query(query)
            # TODO: Add logging at debug level
            if self.table_contract.watermark_columns and len(self.table_contract.watermark_columns) > 1:
                # dropping column 'watermark_column' which is having max timestamp if there are multiple timestamp columns
                data = data.drop("_watermark_column_")
            return data

    def __build_condition(self) -> str:
        """
        **Step 5: Build condition (if watermark columns are present)**
        Description: If watermark columns are present in the `self.table_contract` object, the method builds a condition clause using the `__build_condition` method.
        Details: The condition clause is constructed based on the watermark columns, and is used to filter the data retrieved from the SQL Server database.
        :return:
        """

        if not self.table_contract.watermark_columns or self.table_contract.full_load == True or self.table_contract.load_type == "one_time":
            return ""
        elif len(self.table_contract.watermark_columns) == 1:
            return (
                    f" WHERE {self.table_contract.watermark_columns[0]} >= '{self.table_contract.lower_bound}' "
                    + f"AND {self.table_contract.watermark_columns[0]} < '{self.table_contract.upper_bound}'"
            )
        else:
            return (
                    f" WHERE _watermark_column_ >= '{self.table_contract.lower_bound}' " + f"AND _watermark_column_ < '{self.table_contract.upper_bound}'"
            )

    def __build_select_query(self) -> str:
        """
        **Step 4: Build select query (if no invalid types)**
        Description: If no invalid types are present, the method builds a select query using the `__build_select_query` method.
        Details: The select query is constructed based on the table and schema information, and is used to retrieve data from the SQL Server database.

        :return:
        """
        table = self.table_contract.table_name
        schema = self.table_contract.schema_name
        if not self.table_contract.watermark_columns or self.table_contract.full_load == True or len(self.table_contract.watermark_columns) == 1:
            return f"SELECT * FROM {schema}.{table}"
        else:
            watermark_columns = ", ".join([f"({col})" for col in self.table_contract.watermark_columns])
            return f"""
                    SELECT *
                    FROM (
                        SELECT 
                        *, 
                        (SELECT MAX(_watermark_column_)
                            FROM (VALUES {watermark_columns}) AS last_updated(_watermark_column_)) 
                        AS _watermark_column_
                    
                    """
