from pyspark.sql import DataFrame
from delta.tables import DeltaTable

from ingestion_lib.utils.table_contract import TableContract


def get_row_count_written(df: DataFrame, location: str = None, table_name: str = None) -> int:
    """
    Unchecked
    :param df:
    :param location:
    :param table_name:
    :return:
    """
    if not (location or table_name):
        raise ValueError("Location or table_name must be provided.")
    if location and table_name:
        raise ValueError("Only one of the location or table_name arguments must be provided.")
    spark = df.sparkSession
    if location:
        delta_table = DeltaTable.forPath(spark, location)
    else:
        delta_table = DeltaTable.forName(spark, table_name)

    # Get the latest 5 operations from the history of the Delta Table
    history = delta_table.history(5).collect()
    if not history:
        # cannot use logger here due to circular dependency
        print(f"Cannot get written row count. History is empty. Table: {table_name}, Location: {location}")
        return -1
    for history_record in history:  # History is in descending order by default
        if (
                "operation" in history_record
                and (history_record["operation"] == "WRITE" or history_record["operation"] == "CREATE TABLE AS SELECT")
                and "operationMetrics" in history_record
                and "numOutputRows" in history_record["operationMetrics"]
        ):
            try:
                return int(history_record["operationMetrics"]["numOutputRows"])
            except (ValueError, TypeError):
                print(
                    f"Cannot get written row count. Number of output rows is not an integer. "
                    f"Table: {table_name}, Location: {location}")
                return -1
    print(
        f"Cannot get written row count. Cannot find number of output rows in history. "
        f"Table: {table_name}, Location: {location}")
    return -1


def get_delta_write_options(table_contract: TableContract) -> dict:
    """
    Returns dictionary with replaceWhere condition if watermark column exists and full load is set to false.
    Returns empty dictionary if there is no watermark column mentioned or full load is set to true.
    """
    if not table_contract.watermark_columns or table_contract.full_load == "true" or table_contract.load_type == "one_time":
        return {"mergeSchema": True}
    elif len(table_contract.watermark_columns) == 1:
        return {
            "replaceWhere":
                f"{table_contract.watermark_columns[0]} >= '{table_contract.lower_bound}' "
                f"AND {table_contract.watermark_columns[0]} <= '{table_contract.upper_bound}'",
            "mergeSchema": True,
        }
    else:
        watermark_columnss = ", ".join(table_contract.watermark_columns)
        return {
            "replaceWhere":
                f"greatest({watermark_columnss}) >= '{table_contract.lower_bound}' "
                f"AND greatest({watermark_columnss}) <= '{table_contract.upper_bound}'",
            "mergeSchema": True,
        }
