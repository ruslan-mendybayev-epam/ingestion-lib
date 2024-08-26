import unittest
import httpx
from unittest.mock import MagicMock, patch

from anyio import current_time
from pyspark.sql import SparkSession

from ingestion_lib.extractors.rest_api import HolmanRestAPIExtractor, RestAPIExtractor
from ingestion_lib.utils.data_contract import APIDataContract, DbCredentials


class TestRestApiExtractor(unittest.TestCase):
    @patch('httpx.Client')
    def setUp(self, mock_client):
        self.db_credentials = DbCredentials(
            user='test_user',
            password='test_password',
            jdbc_url='jdbc:oracle:thin:@localhost:1521:xe'
        )
        self.data_contract = APIDataContract(
            batch_timestamp="1970-01-01",
            base_url='http://api.example.com',
            api_key='test_api_key',
            spec_path='path/to/swagger',
            endpoint_url='http://api.example.com/data',
            model='Inventory',
            credentials=self.db_credentials
        )

        self.spark = MagicMock()
        self.extractor = HolmanRestAPIExtractor(self.data_contract, self.spark)
        self.mock_http_client = MagicMock(auto_spec=httpx.Client)
        mock_client.return_value = self.mock_http_client

    def test_creds_success(self):
        expected_token = {'Authorization': 'Bearer test_token'}
        self.mock_http_client.post.return_value.status_code = 200
        self.mock_http_client.post.return_value.json.return_value = {'token': 'test_token'}

        actual_token = self.extractor.auth_header(self.mock_http_client)  # This method needs to be defined in APIExtractor

        # Assertions: Check the results and interactions
        self.assertEqual(actual_token, expected_token, "The token returned should match the expected token.")
        self.mock_http_client.post.assert_called_once()  # Ensuring the post method is called exactly once

    @patch.object(RestAPIExtractor, 'init_client')
    def test_extract_data(self, mock_init_client):
        # Set up the mock client
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        # Create a mock contract and spark session

        # Create an instance of the RestAPIExtractor
        extractor = HolmanRestAPIExtractor(self.data_contract, self.spark)

        # Mock the abstract methods
        extractor.auth_header = MagicMock(return_value={"Authorization": "Bearer token"})
        extractor.get_collection = MagicMock(return_value=["data1", "data2"])
        extractor.convert_collection = MagicMock(return_value=["converted_data1", "converted_data2"])

        # Call the method under test
        result = extractor.extract_data()

        # Assertions
        mock_init_client.assert_called_once()
        extractor.auth_header.assert_called_once_with(mock_client)
        extractor.get_collection.assert_called_once_with({"Authorization": "Bearer token"}, mock_client)
        extractor.convert_collection.assert_called_once_with(["data1", "data2"])
        self.assertEqual(result, ["converted_data1", "converted_data2"])
