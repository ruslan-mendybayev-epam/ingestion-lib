import logging
import os
import shutil
import tempfile

import findspark
import psutil
import pytest
from pyspark.sql import SparkSession
from tests.test_settings import JARS_PACKAGES, DRIVER_JAR_PATH

findspark.init()


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.INFO)


def remove_metastore(temp_dir: str):
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


def get_spark_driver_memory(percentage: int = 80) -> str:
    # Get the free memory in bytes
    free_memory = psutil.virtual_memory().available
    # Calculate percentage of the free memory, and convert bytes to gigabytes
    memory_to_allocate = (free_memory * percentage / 100) // (1024**3)
    # Ensure it's rounded down to the nearest whole number
    memory_to_allocate = int(memory_to_allocate)
    memory_to_allocate_gb = f"{memory_to_allocate}g"
    logging.getLogger("py4j").info(f"Allocating {percentage}% ({memory_to_allocate_gb}) of memory to Spark Driver.")
    return memory_to_allocate_gb


@pytest.fixture(scope="session")
def spark_session(request) -> SparkSession:  # type: ignore
    """Create a PySpark SparkSession."""

    with tempfile.TemporaryDirectory() as temp_dir:
        warehouse_location = f"{temp_dir}/spark-warehouse"
        print(f"\n----------> Warehouse location: {warehouse_location}")
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("ingestion-lib-test")
            .config("spark.sql.warehouse.dir", warehouse_location)
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={warehouse_location};create=true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", JARS_PACKAGES)
            .config("spark.jars", DRIVER_JAR_PATH)
            .config("spark.driver.extraClassPath", DRIVER_JAR_PATH)
            .config("spark.executor.extraClassPath", DRIVER_JAR_PATH)
            .config("spark.driver.memory", get_spark_driver_memory(80))  # set to 80% of free memory
            .getOrCreate()
        )
        request.addfinalizer(lambda: spark.stop())

        quiet_py4j()
        yield spark
        spark.stop()
        remove_metastore(temp_dir)
