from abc import ABC, abstractmethod


class Extractor(ABC):
    def __init__(self, data_contract):
        self.data_contract = data_contract

    @abstractmethod
    def extract_data(self):
        pass
