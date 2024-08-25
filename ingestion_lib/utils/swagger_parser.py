import json
from typing import Dict, Any, Type, Optional
from pydantic import BaseModel, create_model

def load_swagger_file(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as file:
        return json.load(file)



def generate_pydantic_models(components: Dict[str, Any]) -> Dict[str, Type[BaseModel]]:
    models = {}
    type_mapping = {
        'string': str,
        'integer': int,
        'number': float,
        'boolean': bool,
        'array': list,
        'object': dict,
    }

    for name, schema in components['schemas'].items():
        fields = {}
        if 'properties' in schema:
            for prop, type_spec in schema['properties'].items():
                # Handle nullable types
                field_type = type_mapping.get(type_spec['type'], str)  # Default to str if type not found
                if type_spec.get('nullable'):
                    field_type = Optional[field_type]  # Use Optional for nullable types
                fields[prop] = (field_type, ...)
        models[name] = create_model(name, **fields)
    return models
