import os

base_dir = os.path.dirname(os.path.abspath(__file__))
DRIVER_JAR_PATH = f""
JARS_PACKAGES = "io.delta:delta-spark_2.12:3.2.0"
SQL_SERVER_IMAGE = "mcr.microsoft.com/mssql/server:2019-latest"
DB_NAME = "TEST_DB"
DDL_FOLDER = f"{base_dir}/db_scripts/ddl/"
DML_FOLDER = f"{base_dir}/db_scripts/dml/"
DB_SCRIPT = f"{base_dir}/db_scripts/db/db.sql"
LOG_ENDPOINT = "https://eastus.api.example.loganalytics/"
RULE_ID = "123-dcr-metrics-id"
STREAM_NAME = "stream-name-cl"
