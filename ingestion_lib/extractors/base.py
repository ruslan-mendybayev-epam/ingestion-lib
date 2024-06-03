from abc import ABC, abstractmethod

from ingestion_lib.utils.data_contract import DataContract
from pyspark.sql.session import SparkSession, DataFrame


class Extractor(ABC):
    def __init__(self, data_contract: DataContract, spark: SparkSession):
        self.data_contract = data_contract
        self.spark = spark

    @abstractmethod
    def creds(self):
        # TODO: I'm not sure about this being abstract. Probably better to parse proper section from contract
        pass

    @abstractmethod
    def load_data_query(self, query: str):
        pass

    def extract_data(self) -> DataFrame:
        """
        :return:
        """
        # TODO: Add invalid type checks
        select_query = self.__build_select_query()
        condition = self.__build_condition()
        query = f"{select_query}{condition}"
        data = self.load_data_query(query)
        # TODO: Add logging at debug level
        if self.data_contract.watermark_columns and len(self.data_contract.watermark_columns) > 1:
            # dropping column 'watermark_column' which is having max timestamp if there are multiple timestamp columns
            data = data.drop("_watermark_column_")
        return data

    def __build_condition(self) -> str:
        """
        **Step 5: Build condition (if watermark columns are present)**
        Description: If watermark columns are present in the `self.data_contract` object, the method builds a condition clause using the `__build_condition` method.
        Details: The condition clause is constructed based on the watermark columns, and is used to filter the data retrieved from the SQL Server database.
        :return:
        """

        if not self.data_contract.watermark_columns or self.data_contract.full_load == "true" or self.data_contract.load_type == "one_time":
            return ""
        elif len(self.data_contract.watermark_columns) == 1:
            return (
                    f" WHERE {self.data_contract.watermark_columns[0]} >= '{self.data_contract.lower_bound}' "
                    + f"AND {self.data_contract.watermark_columns[0]} < '{self.data_contract.upper_bound}'"
            )
        else:
            return (
                    f" WHERE _watermark_column_ >= '{self.data_contract.lower_bound}' " + f"AND _watermark_column_ < '{self.data_contract.upper_bound}'"
            )

    def __build_select_query(self) -> str:
        """
        **Step 4: Build select query (if no invalid types)**
        Description: If no invalid types are present, the method builds a select query using the `__build_select_query` method.
        Details: The select query is constructed based on the table and schema information, and is used to retrieve data from the SQL Server database.

        :return:
        """
        table = self.data_contract.table_name
        schema = self.data_contract.schema
        if not self.data_contract.watermark_columns or self.data_contract.full_load == "true" or len(self.data_contract.watermark_columns) == 1:
            return f"SELECT * FROM [{schema}].[{table}]"
        else:
            watermark_columns = ", ".join([f"({col})" for col in self.data_contract.watermark_columns])
            return f"""
                    SELECT *
                    FROM (
                        SELECT 
                        *, 
                        (SELECT MAX(_watermark_column_)
                            FROM (VALUES {watermark_columns}) AS last_updated(_watermark_column_)) 
                        AS _watermark_column_
                    
                    """
