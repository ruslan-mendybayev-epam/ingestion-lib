from ingestion_lib.extractors.base import Extractor


class DatabricksIngestionProvider:
    def __init__(self, extractor: Extractor):
        self.extractor = extractor

    def normalize_data(self, data):
        # Common normalization logic
        pass

    def write_data(self, data):
        # Logic to write data to Databricks Delta Lake
        pass

    def execute_ingestion(self):
        data = self.extractor.extract_data()
        normalized_data = self.normalize_data(data)
        self.write_data(normalized_data)
