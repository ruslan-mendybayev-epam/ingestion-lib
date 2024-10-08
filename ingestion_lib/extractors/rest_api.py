from abc import abstractmethod
from typing import Union

import httpx
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from ingestion_lib.extractors.base import Extractor
from ingestion_lib.model.base import SchemaFactory
from ingestion_lib.model.holman import HOLMAN_VEHICLES_SCHEMA
from ingestion_lib.utils.data_contract import TableContract, APIDataContract


class RestAPIExtractor(Extractor):

    def __init__(self, contract: APIDataContract, spark: SparkSession):
        super().__init__(contract, spark)


    def extract_data(self):
        client = self.init_client()
        header = self.auth_header(client)
        collection = self.get_collection(header, client)
        return self.convert_collection(collection)

    def init_client(self):
        return httpx.Client(base_url=self.data_contract.base_url)

    @abstractmethod
    def auth_header(self, client):
        pass

    @abstractmethod
    def get_collection(self, header, client):
        pass

    def convert_collection(self, collection):
        return collection

class HolmanRestAPIExtractor(RestAPIExtractor):

    def get_collection(self, header, client):
        url = f"{self.data_contract.base_url}/{self.data_contract.endpoint_url}"
        model_name = self.data_contract.model
        response = client.get(url, headers=header)
        initial_data = response.json()
        result = initial_data[model_name]

        total_pages = int(initial_data.get('totalPages', 1))

        for page in range(2, total_pages + 1):
            response =client.get(f"{url}?pageNumber={page}", headers=header)
            response.raise_for_status()
            page_data = response.json()
            result.extend(page_data[model_name])
        return result

    def auth_header(self, client):
        """
        Authenticate with the API and return the token.
        """
        url = f"{self.data_contract.base_url}/v1/users/authenticate"
        headers = {"Content-Type": "application/json"}
        payload = {
            "userName": self.data_contract.credentials.user,
            "password": self.data_contract.credentials.password
        }

        response = client.post(url=url, json=payload, headers=headers)
        if response.status_code == 200:
            print("Authentication successful")
            return {'Authorization': f'Bearer {response.json().get("token")}'}
        else:
            print("Failed to authenticate")
            print("Status code:", response.status_code)
            print("Response:", response.text)
        return None

    def convert_collection(self, collection):
        schema = SchemaFactory.get_schema(self.data_contract.table_name)
        df = self.spark.createDataFrame(collection, schema=schema)
        return df

    def __init__(self, contract: APIDataContract, spark: SparkSession):
        super().__init__(contract, spark)
