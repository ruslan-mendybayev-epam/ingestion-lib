import glob
import os
from unittest.mock import Mock

from azure.monitor.ingestion import LogsIngestionClient

from tests.db_test_settings import LOG_ENDPOINT


def assert_column_value(df, column_name, value):
    first_row = df.first()
    if first_row:
        found_value = first_row[column_name]
        assert value == found_value, f"Expected '{value}' but found '{found_value}' in column '{column_name}'."
    else:
        raise AssertionError(f"No rows found in DataFrame. Expected '{value}' in column '{column_name}'.")


def get_mock_db_utils(db_user, db_password):
    def mock_dbutils_secrets_get(scope, key):
        secrets = {"mssql-admin-username": db_user, "mssql-admin-password": db_password, "dce-dev-url": LOG_ENDPOINT}
        return secrets[key]

    mock_dbutils = Mock()
    mock_dbutils.secrets.get.side_effect = mock_dbutils_secrets_get
    return mock_dbutils


def async_return():
    async def fake_async_func():
        return None

    return fake_async_func()


def get_mock_logs_ingestion_client(mock_logs_ingestion_client_class):
    mock_logs_ingestion_client = Mock(spec=LogsIngestionClient)
    mock_logs_ingestion_client.upload.return_value = async_return()
    mock_logs_ingestion_client_class.return_value = mock_logs_ingestion_client

    return mock_logs_ingestion_client


def create_database(oracle, db_path, ddl_path, dml_path):
    """
    Creates a database, tables, and inserts data into the tables.

    Args:
        oracle (object): An object representing the MSSQL connection.
        db_path (str): The path to the file containing the database creation script.
        ddl_path (str): The path to the directory containing the DDL scripts for creating tables.
        dml_path (str): The path to the directory containing the DML scripts for inserting data.
        url: oracle jdbs url

    Returns:
        None
    """
    url = f"system/{oracle.oracle_password}@//localhost:{oracle.get_exposed_port(1521)}/XE"
    # create db
    with open(f"{db_path}", "r") as file:
        db_file = file.read()
        oracle.exec(["sh", "-c", f'echo "{db_file}" | sqlplus {url}'])

    # create tables
    for script_file in glob.glob(os.path.join(ddl_path, "**/*.sql"), recursive=True):
        with open(script_file, "r") as file:
            oracle.exec(["sh", "-c", f'echo "{file.read()}" | sqlplus {url}'])

    # insert data into tables
    for script_file in glob.glob(os.path.join(dml_path, "**/*.sql"), recursive=True):
        with open(script_file, "r") as file:
            oracle.exec(["sh", "-c", f'echo "{file.read()}" | sqlplus {url}'])
