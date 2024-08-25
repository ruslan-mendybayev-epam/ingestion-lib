from .base import Extractor
from ..utils.data_contract import DataContract, APIDataContract
from ..utils.swagger_parser import load_swagger_file, generate_pydantic_models
import httpx


class APIExtractor(Extractor):
    def load_data_query(self, query: str):
        pass

    def __init__(self, data_contract: APIDataContract, spark):
        """
        Initialize the APIExtractor with data contract and Spark session.
        """
        super().__init__(data_contract, spark)
        self.swagger_data = load_swagger_file(data_contract.swagger_path)
        self.models = generate_pydantic_models(self.swagger_data['components'])
        self.client = httpx.Client(base_url=data_contract.base_url)

    def creds(self):
        """
        Authenticate with the API and return the token.
        """
        url = f"{self.data_contract.base_url}/v1/users/authenticate"
        headers = {"Content-Type": "application/json"}
        payload = {
            "userName": self.data_contract.credentials.user,
            "password": self.data_contract.credentials.password
        }

        response = self.client.post(url=url, json=payload, headers=headers)
        if response.status_code == 200:
            print("Authentication successful")
            return response.json().get("token")
        else:
            print("Failed to authenticate")
            print("Status code:", response.status_code)
            print("Response:", response.text)
        return None

    def extract_data(self):
        """
        Extract data using the API, authenticate first to get the token.
        """
        token = self.creds()
        if not token:
            raise Exception("Authentication failed, token not retrieved.")

        headers = {'Authorization': f'Bearer {token}'}
        url = self.data_contract.endpoint_url
        model_name = self.data_contract.endpoint_model
        # model = self.models.get(model_name)

        # if not model:
        #     raise ValueError(f"Model {model_name} not found in Swagger definitions.")

        response = self.client.get(url, headers=headers)
        response.raise_for_status()
        initial_data = response.json()

        total_pages = initial_data.get('totalPages', 1)
        inventory_df = self.spark.read.json(self.spark.sparkContext.parallelize([initial_data[model_name]]))

        for page in range(2, total_pages + 1):
            response = self.client.get(f"{url}?pageNumber={page}", headers=headers)
            response.raise_for_status()
            page_data = response.json()
            page_df = self.spark.read.json(self.spark.sparkContext.parallelize([page_data[model_name]]))
            inventory_df = inventory_df.union(page_df)

        return inventory_df
