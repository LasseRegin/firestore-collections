from datetime import datetime
from typing import Any, List, Union, Tuple, Optional

from bson import ObjectId
from firebase_admin.exceptions import NotFoundError, ALREADY_EXISTS
from google.cloud.firestore_v1.collection import CollectionReference
from pydantic import BaseModel

from firestore_collections.client import client
from firestore_collections.enums import OrderByDirection
from firestore_collections.utils import chunks, parse_document_to_dict


class Collection:
    def __init__(self, schema: BaseModel):
        self.schema = schema
        self.collection_name = schema.__collection_name__

        if self.collection_name is None:
            raise ValueError('`__collection_name__` has not been defined')

    @property
    def collection(self) -> CollectionReference:
        return client.collection(self.collection_name)

    def get(self, id: str) -> Any:
        doc_ref = self.collection.document(id)
        doc = doc_ref.get()

        if not doc.exists:
            raise NotFoundError(f"Document {self.collection_name}.{id} could not be found")

        return self.schema(**{**doc.to_dict(), 'id': doc.id})

    def get_all(self,
                limit: Optional[int] = None,
                order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
                ) -> List[Any]:
        return self._query(conditions=[], limit=limit, order_by=order_by)

    def get_by_attribute(self, attribute: str, value: Any) -> Any:
        docs = self.query_by_attribute(attribute=attribute, value=value)
        if len(docs) == 0:
            raise NotFoundError(f"Document could not be found in {self.collection_name} with `{attribute}=={value}`")

        return docs[0]

    def _query(self,
               conditions: List[Tuple[str, str, Any]],
               limit: Optional[int] = None,
               order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
               ) -> List[Any]:
        # Sanity checks
        operators = [x[1].lower() for x in conditions]
        unique_operators = list(set(operators))
        if u'in' in operators and len(operators) > 1:
            raise ValueError('Using `in` operator cannot have multiple conditions')
        if len(unique_operators) == 1:
            operator = unique_operators[0]
            if len(operators) > 1 and operator != u'==':
                raise ValueError('Can only have multiple operators with operator `==`')
            if operator == 'in':
                attribute, operator, values = conditions[0]
                return self._query_in(
                    attribute=attribute,
                    values=values,
                    limit=limit,
                    order_by=order_by)
        if len(unique_operators) > 1:
            raise ValueError('Cannot have different operators in same query')

        # Init docs object
        docs = self.collection

        # Add conditions (where clauses)
        for condition in conditions:
            attribute, operator, value = condition
            docs = docs.where(attribute, operator, value)

        # Order by if provided
        for order_by_tuple in order_by:
            attribute, direction_enum = order_by_tuple
            docs = docs.order_by(attribute, direction=direction_enum.value)

        # Limit result if provided
        if limit:
            docs = docs.limit(limit)

        # Create generator
        docs = docs.stream()

        return [
            self.schema(**{**doc.to_dict(), 'id': doc.id})
            for doc in docs
        ]

    def _query_in(self,
                  attribute: str,
                  values: Any,
                  limit: Optional[int] = None,
                  order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
                  ) -> List[Any]:
        # Split values up in N lists of max 10
        # since Firestore limits the `in` operator
        # to max 10 values
        values_lists = list(chunks(values, n=10))

        if len(values_lists) > 10:
            raise ValueError('Too many values provided for `in` query')

        if len(order_by) > 0:
            raise NotImplementedError('`order_by` has not been implemented yet')

        docs_all = []
        for values in values_lists:
            # Init docs object
            docs = self.collection

            # Add conditions (where clauses)
            docs = docs.where(attribute, u'in', values)

            # Limit result if provided
            if limit:
                docs = docs.limit(limit)

            # Create generator
            docs = [
                self.schema(**{**doc.to_dict(), 'id': doc.id})
                for doc in docs.stream()
            ]
            docs_all.extend(docs)

        return docs_all

    def query_by_attribute(self,
                           attribute: str,
                           value: Any,
                           operator: Optional[str] = u'==',
                           limit: Optional[int] = None,
                           order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
                           ) -> List[Any]:
        return self._query(
            conditions=[(attribute, operator, value)],
            limit=limit,
            order_by=order_by)

    def update(self, doc: Union[BaseModel, dict]) -> None:
        if isinstance(doc, BaseModel) and not isinstance(doc, self.schema):
            raise ValueError(f"Invalid schema used for provided document: {doc}")

        if isinstance(doc, dict):
            doc = self.schema(doc)

        if doc.id is None:
            raise ValueError(f"Provided document has not id: {doc}")

        # Set updated date
        doc.updated_at = datetime.utcnow()

        # Convert from schema to dictionary
        doc_id = doc.id
        doc = parse_document_to_dict(doc=doc)

        # Get document reference
        doc_ref = self.collection.document(doc_id)

        # The `merge` mergeds the new data with any existing document
        # to avoid overwriting entire documents.
        doc_ref.set(doc, merge=True)

    def insert(self, doc: Union[BaseModel, dict]) -> BaseModel:
        if isinstance(doc, BaseModel) and not isinstance(doc, self.schema):
            raise ValueError(f"Invalid schema used for provided document: {doc}")

        if isinstance(doc, dict):
            doc = self.schema(doc)

        # Set created date
        doc.created_at = datetime.utcnow()

        # Convert from schema to dictionary
        doc = parse_document_to_dict(doc=doc)

        # Insert new document
        new_id = doc.pop('id', None)
        if new_id is None:
            new_id = str(ObjectId())
        else:
            # Check if document already exists
            if self.collection.document(new_id).get().exists:
                raise ALREADY_EXISTS(f"{self.schema.__name__} already exists with ID: {new_id}")
        doc_ref = self.collection.document(new_id)
        doc_ref.set(doc)

        # Retrieve document snapshot
        doc = doc_ref.get()

        return self.schema(**{**doc.to_dict(), 'id': doc.id})

    def delete(self, id: str) -> None:
        self.collection.document(id).delete()
