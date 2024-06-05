import os

base_dir = os.path.dirname(os.path.abspath(__file__))
DRIVER_JAR_PATH = f"{base_dir}/jars/ojdbc10.jar"
DB_NAME = "TEST_DB"
DDL_FOLDER = f"{base_dir}/db_scripts/ddl/"
DML_FOLDER = f"{base_dir}/db_scripts/dml/"
DB_SCRIPT = f"{base_dir}/db_scripts/db/db.sql"
LOG_ENDPOINT = "https://eastus.api.example.loganalytics/"
RULE_ID = "123-dcr-metrics-id"
STREAM_NAME = "stream-name-cl"
