from pyspark.sql import SparkSession

from ingestion_lib.extractors.rest_api import HolmanRestAPIExtractor
from ingestion_lib.utils.data_contract import DbCredentials, APIDataContract

table_details = {'extractor': 'holman.py',
                 'base_url': 'https://customer-experience-api.arifleet.com',
                 'api_key': 'your_api_key_here',
                 'spec_path': 'https://customer-experience-api.arifleet.com/swagger/1.0/swagger.json',
                 'endpoint_url': 'v1/vehicles',
                 'scope': 'external',
                 'env_key': 'env-name',
                 'target_schema': 'bronze_holman',
                 'table_name': 'vehicles',
                 'batch_timestamp': '20',
                 'model': 'inventory',
                 'credentials': DbCredentials(user='', password='', jdbc_url='')}
contract = APIDataContract(**table_details)
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Holman Extractor") \
    .getOrCreate()
holman = HolmanRestAPIExtractor(contract, spark)
holman.extract_data().count()
