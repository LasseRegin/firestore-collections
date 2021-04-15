from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, SecretStr


def chunks(lst: List[Any], n: int) -> List[List[Any]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parse_document_to_dict(doc: BaseModel) -> Dict[str, Any]:
    # Convert to dictionary
    doc = doc.dict()

    # Check for any secret values and enum values
    for key, value in doc.items():
        if isinstance(value, SecretStr):
            doc[key] = value.get_secret_value()
        elif isinstance(value, Enum):
            doc[key] = value.value

    return doc


def parse_attributes_to_dict(attributes: Dict[str, Any]) -> Dict[str, Any]:
    # Check for any secret values and enum values
    for key, value in attributes.items():
        if isinstance(value, SecretStr):
            attributes[key] = value.get_secret_value()
        elif isinstance(value, Enum):
            attributes[key] = value.value

    return attributes
