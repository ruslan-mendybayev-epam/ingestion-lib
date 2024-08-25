import unittest
from unittest.mock import patch, MagicMock, mock_open

import httpx

from ingestion_lib.extractors.rest_api import APIExtractor  # Adjust the import as necessary
from ingestion_lib.utils.data_contract import APIDataContract, DbCredentials


class TestAPIExtractor(unittest.TestCase):

    @patch('httpx.Client')
    @patch('ingestion_lib.utils.swagger_parser.generate_pydantic_models')
    def setUp(self, mock_generate_models, mock_client):
        # Mocking the database credentials
        self.db_credentials = DbCredentials(
            user='test_user',
            password='test_password',
            jdbc_url='jdbc:oracle:thin:@localhost:1521:xe'
        )

        # Mocking the API data contract
        self.data_contract = APIDataContract(
            base_url='http://api.example.com',
            api_key='test_api_key',
            swagger_path='path/to/swagger',
            endpoint_url='http://api.example.com/data',
            endpoint_model='Inventory',
            credentials=self.db_credentials
        )

        self.spark = MagicMock()
        swagger_file_content = '''{
          "openapi": "3.0.1",
          "info": {
            "version": "1.0"
          },
          "paths": {
            "/v1/inbound/accidents": {
              "post": {
                "tags": ["Accident"],
                "operationId": "PostAccidents",
                "requestBody": {
                  "content": {
                    "application/json": {
                      "schema": {"nullable": true}
                    }
                  }
                },
                "responses": {
                  "200": {"description": "Success"}
                }
              }
            }
          },
          "components": {
            "schemas": {
              "testmodel": {
                "type": "object",
                "properties": {
                  "item": {"type": "string", "nullable": true}
                },
                "additionalProperties": false
              },
              "AuthenticateRequest": {
                "required": ["password", "username"],
                "type": "object",
                "properties": {
                  "username": {"type": "string"},
                  "password": {"type": "string"}
                },
                "additionalProperties": false
              }
            }
          }
        }'''

        self.mock_http_client = MagicMock(auto_spec=httpx.Client)
        mock_client.return_value = self.mock_http_client

        with patch('builtins.open', mock_open(read_data=swagger_file_content)):
            # Initialize APIExtractor
            self.extractor = APIExtractor(self.data_contract, self.spark)

        # Mocking the return value of generate_pydantic_models
        mock_generate_models.return_value = {
            'TestModel': MagicMock()  # Mocking the Pydantic model
        }



    def test_creds_success(self):
        # Mocking the response for successful authentication
        mock_client = MagicMock()
        with patch.object(self.extractor, "client", mock_client):
            mock_client.post.return_value.status_code = 200
            mock_client.post.return_value.json.return_value = {'token': 'test_token'}
            token = self.extractor.creds()
            self.assertEqual(token, 'test_token')
            mock_client.post.assert_called_once()

    def test_creds_success_annon(self):
        # Setup: Configure the mock client class
        expected_token = 'test_token'
        self.mock_http_client.post.return_value.status_code = 200
        self.mock_http_client.post.return_value.json.return_value = {'token': expected_token}

        # Assuming 'creds' method exists and uses 'client.post' to fetch a token
        actual_token = self.extractor.creds()  # This method needs to be defined in APIExtractor

        # Assertions: Check the results and interactions
        self.assertEqual(actual_token, expected_token, "The token returned should match the expected token.")
        self.mock_http_client.post.assert_called_once()  # Ensuring the post method is called exactly once


    def test_creds_failure(self):
        # Mocking the response for failed authentication
        self.mock_http_client.post.return_value.json.return_value = None
        self.mock_http_client.post.return_value.status_code = 401
        self.mock_http_client.post.return_value.text = 'Unauthorized'

        token = self.extractor.creds()
        self.assertIsNone(token)
        self.mock_http_client.post.assert_called_once()

    def test_extract_data_success(self):
        # Mocking successful authentication
        self.mock_http_client.post.return_value.status_code = 200
        self.mock_http_client.post.return_value.json.return_value = {'token': 'test_token'}

        # Mocking successful data retrieval
        self.mock_http_client.get.return_value.status_code = 200
        self.mock_http_client.get.return_value.json.return_value = {
            'totalPages': 1,
            'Inventory': [{'item': 'value'}]
        }

        # Mocking Spark DataFrame creation
        self.spark.read.json.return_value = MagicMock()

        inventory_df = self.extractor.extract_data()
        self.assertIsNotNone(inventory_df)
        self.mock_http_client.post.assert_called_once()
        self.mock_http_client.get.assert_called_once()


if __name__ == '__main__':
    unittest.main()
