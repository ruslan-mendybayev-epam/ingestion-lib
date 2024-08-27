from ingestion_lib.model.holman import HOLMAN_VEHICLES_SCHEMA


class SchemaCreator:
    def create_schema(self):
        raise NotImplementedError("Subclasses must implement create_schema method")


class VehicleSchemaCreator(SchemaCreator):
    def create_schema(self):
        return HOLMAN_VEHICLES_SCHEMA


class SchemaFactory:
    @staticmethod
    def get_schema(property_value):
        if property_value == "vehicles":
            return VehicleSchemaCreator().create_schema()
        # Add more conditions for different schema types
        else:
            raise ValueError(f"Unknown property value: {property_value}")
