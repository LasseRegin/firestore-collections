from typing import Optional

from pydantic import BaseModel

from firestore_collections.enums import FirestoreTimestamp


class Schema(BaseModel):
    __collection_name__ = None

    id: Optional[str]
    created_at: Optional[FirestoreTimestamp]
    updated_at: Optional[FirestoreTimestamp]


class SchemaWithOwner(Schema):
    created_by: Optional[str]
    updated_by: Optional[str]


class StaticSchema(BaseModel):
    __collection_name__ = None

    id: Optional[str]
    created_at: Optional[FirestoreTimestamp]


class StaticSchemaWithOwner(StaticSchema):
    created_by: Optional[str]
