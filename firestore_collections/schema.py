from typing import Optional

from pydantic import BaseModel

from firestore_collections.enums import FirestoreTimestamp


class Schema(BaseModel):
    __collection_name__ = None

    id: Optional[str]
    created_at: Optional[FirestoreTimestamp]
    updated_at: Optional[FirestoreTimestamp]
