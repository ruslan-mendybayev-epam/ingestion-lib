from abc import ABC, abstractmethod

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from ingestion_lib.utils.data_contract import TableContract


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
    if not table_contract.watermark_columns or table_contract.full_load or table_contract.load_type == "one_time":
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


class DatabricksWorkflow(ABC):
    def __init__(self, input_path, template, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.template = template

    def read_yaml(self, file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

    def write_yaml(self, data, file_path):
        with open(file_path, 'w') as file:
            yaml.safe_dump(data, file, default_flow_style=False, sort_keys=False)

    @abstractmethod
    def update_jobs(self):
        pass


class DatabricksJobUpdater(DatabricksWorkflow):
    input_path:  str
    template: str
    output_path: str

    def transform_dataset_name(self, name):
        return name.replace('.', '_')

    def update_jobs(self):
        ingestion_data = self.read_yaml(self.input_path)
        datasets = ingestion_data.get('datasets', [])

        databricks_job_data = self.read_yaml(self.template)
        tasks_section = databricks_job_data.setdefault('resources', {}).setdefault('jobs', {}).setdefault('ingestion', {}).setdefault('tasks', [])

        existing_tasks = {task['task_key']: task for task in tasks_section if 'task_key' in task}

        for dataset in datasets:
            task_key = self.transform_dataset_name(dataset['name'])
            task_data = {
                'task_key': task_key,
                'existing_cluster_id': '1234-567890-abcde123',
                'notebook_task': {
                    'notebook_path': './hello.py'
                }
            }
            if task_key in existing_tasks:
                existing_tasks[task_key].update(task_data)
            else:
                existing_tasks[task_key] = task_data

        # Update the tasks list only if there are tasks to add
        if existing_tasks:
            databricks_job_data['resources']['jobs']['ingestion']['tasks'] = list(existing_tasks.values())
        else:
            # Optionally remove the tasks key if no tasks exist
            databricks_job_data['resources']['jobs']['ingestion'].pop('tasks', None)

        self.write_yaml(databricks_job_data, self.output_path)

