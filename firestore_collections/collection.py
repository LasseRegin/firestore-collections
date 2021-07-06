from collections import Counter
from datetime import datetime
from typing import Any, List, Union, Tuple, Optional, Dict

from bson import ObjectId
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.api_core.exceptions import NotFound, Conflict
from google.cloud.firestore_v1.batch import WriteBatch
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore import Client
from pydantic import BaseModel

from firestore_collections.enums import OrderByDirection
from firestore_collections.schema import StaticSchemaWithOwner, SchemaWithOwner
from firestore_collections.utils import (
    chunks,
    parse_attributes_to_dict,
    parse_document_to_dict,
)


class Collection:
    is_updatable = True

    def __init__(self,
                 schema: BaseModel,
                 firestore_client: Optional[Client] = None,
                 force_ownership: Optional[bool] = False):
        self.schema = schema
        self.collection_name = schema.__collection_name__
        self.force_ownership = force_ownership

        if self.collection_name is None:
            raise ValueError('`__collection_name__` has not been defined')

        if firestore_client is None:
            from firestore_collections.client import client
            self._client = client
        else:
            self._client = firestore_client

        if issubclass(self.schema, BaseModel):
            schema_pydantic = self.schema.schema()
            self.schema_props = schema_pydantic.get('properties', {})
            self.is_updatable = self.has_attribute(attribute='updated_at')
        else:
            self.schema_props = None

    @property
    def name(self):
        return self.schema.__name__

    @property
    def requires_owner(self):
        return self.requires_owner_insert

    @property
    def requires_owner_insert(self):
        return (
            self.schema.__mro__[1] == SchemaWithOwner or
            self.schema.__mro__[1] == StaticSchemaWithOwner
        )

    @property
    def requires_owner_update(self):
        return self.schema.__mro__[1] == SchemaWithOwner

    def get_unique_keys(self):
        return getattr(self.schema, '__unique_keys__', [])

    def has_attribute(self, attribute: str) -> bool:
        if self.schema_props is not None:
            return attribute in self.schema_props
        return True

    @property
    def collection(self) -> CollectionReference:
        return self._client.collection(self.collection_name)

    def get(self, id: str) -> Any:
        doc_ref = self.collection.document(id)
        doc = doc_ref.get()

        if not doc.exists:
            raise NotFound(f"Document {self.collection_name}.{id} could not be found")

        return self.schema(**{**doc.to_dict(), 'id': doc.id})

    def get_all(self,
                limit: Optional[int] = None,
                order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
                ) -> List[Any]:
        return self._query(conditions=[], limit=limit, order_by=order_by)

    def get_by_attribute(self, attribute: str, value: Any) -> Any:
        docs = self.query_by_attribute(attribute=attribute, value=value)
        if len(docs) == 0:
            raise NotFound(f"Document could not be found in {self.collection_name} with `{attribute}=={value}`")

        return docs[0]

    def _query(self,
               conditions: List[Tuple[str, str, Any]],
               limit: Optional[int] = None,
               order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
               ) -> List[Any]:
        # Parse condition values based on attribute type
        conditions = self._parse_conditions(conditions)

        # Sanity checks
        operators = [x[1].lower() for x in conditions]
        operator_counts = Counter(operators)
        unique_operators = list(operator_counts.keys())
        in_operator_count = operator_counts.get(u'in', 0)

        if in_operator_count > 1:
            raise ValueError('Cannot use more than one `in` operator in conditions')

        if in_operator_count == 1:
            in_operator_idx = operators.index(u'in')
            in_condition = conditions.pop(in_operator_idx)
            attribute, _, values = in_condition
            return self._query_in(
                attribute=attribute,
                values=values,
                limit=limit,
                additional_attributes=[x[0] for x in conditions],
                additional_values=[x[2] for x in conditions],
                additional_operator=[x[1] for x in conditions],
                order_by=order_by)

        if len(unique_operators) > 1:
            allowed_mixed_operators = {u'>=', u'<=', u'==', u'!=', u'>', u'<', u'in'}
            if len(set(unique_operators) - allowed_mixed_operators) != 0:
                raise ValueError(f"Only following operators can be mixed: {allowed_mixed_operators}")

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
                  additional_attributes: Optional[List[str]] = [],
                  additional_values: Optional[List[Any]] = [],
                  additional_operator: Optional[List[str]] = [],
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

        if len(additional_attributes) != len(additional_values):
            raise ValueError('Size of `additional_attributes` and `additional_values` must match')
        if len(additional_values) != len(additional_operator):
            raise ValueError('Size of `additional_values` and `additional_operator` must match')

        docs_all = []
        for values in values_lists:
            # Init docs object
            docs = self.collection

            # Add conditions (where clauses)
            docs = docs.where(attribute, u'in', values)

            for _attribute, _value, _operator in zip(
                additional_attributes,
                additional_values,
                additional_operator,
            ):
                docs = docs.where(_attribute, _operator, _value)

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

    def query_by_attributes(self,
                            attributes: List[str],
                            values: List[Any],
                            operators: List[str],
                            limit: Optional[int] = None,
                            order_by: Optional[List[Tuple[str, OrderByDirection]]] = []
                            ) -> List[Any]:
        if len(attributes) != len(values):
            raise ValueError('Number af attributes and values provided must be equal')

        return self._query(
            conditions=[
                (attribute, operator, value)
                for attribute, operator, value in zip(attributes, operators, values)
            ],
            limit=limit,
            order_by=order_by)

    def update(self,
               doc: Union[BaseModel, dict],
               owner: Optional[str] = None,
               force: Optional[bool] = False,
               dry_run: Optional[bool] = False) -> None:
        if isinstance(doc, BaseModel) and not isinstance(doc, self.schema):
            raise ValueError(f"Invalid schema used for provided document: {doc}")

        if isinstance(doc, dict):
            doc = self.schema(doc)

        if doc.id is None:
            raise ValueError(f"Provided document has not id: {doc}")

        # Check for any restrictions
        self._check_restrictions(doc=doc, is_update=True)

        if self.is_updatable:
            # Set updated date
            doc.updated_at = datetime.utcnow()

        if self.requires_owner_update:
            if not force and (owner is None and self.force_ownership):
                raise ValueError(f"An `owner` must be defined for collection {self.name}")
            doc.updated_by = owner

        # Convert from schema to dictionary
        doc_id = doc.id
        doc = parse_document_to_dict(doc=doc)

        if dry_run:
            return doc

        # Get document reference
        doc_ref = self.collection.document(doc_id)

        # The `merge` mergeds the new data with any existing document
        # to avoid overwriting entire documents.
        # TODO: Use `doc_ref.update(...)` instead
        # See https://googleapis.dev/python/firestore/latest/document.html?highlight=update#google.cloud.firestore_v1.document.DocumentReference.update
        doc_ref.set(doc, merge=True)

    def update_attribute(self,
                         doc_id: str,
                         attribute: str,
                         value: Any,
                         owner: Optional[str] = None,
                         force: Optional[bool] = False) -> None:
        return self.update_attributes(
            doc_id=doc_id,
            attributes={
                attribute: value
            },
            owner=owner,
            force=force)

    def update_attributes(self,
                          doc_id: str,
                          attributes: Dict[str, Any],
                          owner: Optional[str] = None,
                          force: Optional[bool] = False) -> None:
        if doc_id is None:
            raise ValueError(f"Invalid `doc_id` provided: {doc_id}")

        # Check if valid attribute keys provided
        for key in attributes.keys():
            if not self.has_attribute(attribute=key):
                raise KeyError('Invalid attribute provided: `{key}`')

        # Check for any restrictions
        self._check_restrictions_attributes(
            doc_id=doc_id,
            attributes=attributes)

        # Set updated date
        doc = attributes
        if self.is_updatable:
            doc['updated_at'] = datetime.utcnow()

        if self.requires_owner_update:
            if not force and (owner is None and self.force_ownership):
                raise ValueError(f"An `owner` must be defined for collection {self.name}")
            doc['updated_by'] = owner

        # Parse values
        doc = parse_attributes_to_dict(attributes=doc)

        # Get document reference
        doc_ref = self.collection.document(doc_id)

        # See
        # https://googleapis.dev/python/firestore/latest/document.html?highlight=update#google.cloud.firestore_v1.document.DocumentReference.update
        doc_ref.update(doc)

    def bulk_update(self,
                    docs: List[Union[BaseModel, dict]],
                    owner: Optional[str] = None,
                    force: Optional[bool] = False,
                    merge: Optional[bool] = False,
                    batch_size: Optional[int] = 500) -> None:
        if batch_size <= 0:
            raise ValueError('`batch_size` must be larger than 0')
        if len(docs) == 0:
            raise ValueError('No documents provided')

        # Parse all docs to dicts
        docs = [
            self.update(
               doc=doc,
               owner=owner,
               force=force,
               dry_run=True,
            )
            for doc in docs
        ]

        # Define batch operation
        write_batch = WriteBatch(client=self._client)

        for i, doc in enumerate(docs):
            doc_id = doc.pop('id', None)
            if doc_id is None:
                doc_id = str(ObjectId())

            write_batch.set(
                reference=self.collection.document(doc_id),
                document_data=doc,
                merge=merge)

            if (i + 1) % batch_size == 0:
                # Execute batch operation
                write_batch.commit()
                write_batch = WriteBatch(client=self._client)

        if (i + 1) % batch_size != 0:
            # Execute batch operation
            write_batch.commit()

    def insert(self,
               doc: Union[BaseModel, dict],
               owner: Optional[str] = None,
               force: Optional[bool] = False,
               dry_run: Optional[bool] = False) -> BaseModel:
        if isinstance(doc, BaseModel) and not isinstance(doc, self.schema):
            raise ValueError(f"Invalid schema used for provided document: {doc}")

        if isinstance(doc, dict):
            doc = self.schema(doc)

        # Set created date
        doc.created_at = datetime.utcnow()

        if self.requires_owner_insert:
            if not force and (owner is None and self.force_ownership):
                raise ValueError(f"An `owner` must be defined for collection {self.name}")
            doc.created_by = owner

        # Check for any restrictions
        self._check_restrictions(doc=doc, is_update=False)

        # Convert from schema to dictionary
        doc = parse_document_to_dict(doc=doc)

        if dry_run:
            return doc

        # Insert new document
        new_id = doc.pop('id', None)
        if new_id is None:
            new_id = str(ObjectId())
        doc_ref = self.collection.document(new_id)
        doc_ref.create(doc)
        doc = doc_ref.get()

        return self.schema(**{**doc.to_dict(), 'id': doc.id})

    def bulk_insert(self,
                    docs: List[Union[BaseModel, dict]],
                    owner: Optional[str] = None,
                    force: Optional[bool] = False,
                    batch_size: Optional[int] = 500) -> None:
        if batch_size <= 0:
            raise ValueError('`batch_size` must be larger than 0')
        if len(docs) == 0:
            raise ValueError('No documents provided')

        # Parse all docs to dicts
        docs = [
            self.insert(
               doc=doc,
               owner=owner,
               force=force,
               dry_run=True,
            )
            for doc in docs
        ]

        # Define batch operation
        write_batch = WriteBatch(client=self._client)

        for i, doc in enumerate(docs):
            doc_id = doc.pop('id', None)
            if doc_id is None:
                doc_id = str(ObjectId())

            write_batch.create(
                reference=self.collection.document(doc_id),
                document_data=doc)

            if (i + 1) % batch_size == 0:
                # Execute batch operation
                write_batch.commit()
                write_batch = WriteBatch(client=self._client)

        if (i + 1) % batch_size != 0:
            # Execute batch operation
            write_batch.commit()

    def delete(self,
               id: str,
               owner: Optional[str] = None,
               force: Optional[bool] = False) -> None:
        # Set updated by and time before deleting to trigger change
        if issubclass(self.schema, SchemaWithOwner):
            if not force and (owner is None and self.force_ownership):
                raise ValueError(f"An `owner` must be defined for collection {self.name}")

            if owner is not None:
                if self.is_updatable:
                    self.collection.document(id).set({
                        'updated_at': datetime.utcnow(),
                        'updated_by': owner,
                        'deleted': True,
                    }, merge=True)
                else:
                    self.collection.document(id).set({
                        'deleted': True,
                    }, merge=True)

        self.collection.document(id).delete()

    def bulk_delete(self,
                    doc_ids: List[str],
                    owner: Optional[str] = None,
                    force: Optional[bool] = False,
                    batch_size: Optional[int] = 500) -> None:
        if batch_size <= 0:
            raise ValueError('`batch_size` must be larger than 0')
        if len(doc_ids) == 0:
            raise ValueError('No document IDs provided')

        # Set updated by and time before deleting to trigger change
        update_before_delete = False
        if issubclass(self.schema, SchemaWithOwner):
            if not force and (owner is None and self.force_ownership):
                raise ValueError(f"An `owner` must be defined for collection {self.name}")

            if owner is not None:
                update_before_delete = True

        # Define batch operation
        write_batch = WriteBatch(client=self._client)

        for i, doc_id in enumerate(doc_ids):
            if update_before_delete:
                if self.is_updatable:
                    write_batch.set(
                        reference=self.collection.document(doc_id),
                        document_data={
                            'updated_at': datetime.utcnow(),
                            'updated_by': owner,
                            'deleted': True,
                        },
                        merge=True)
                else:
                    write_batch.set(
                        reference=self.collection.document(doc_id),
                        document_data={
                            'deleted': True,
                        },
                        merge=True)

            write_batch.delete(reference=self.collection.document(doc_id))

            if (i + 1) % batch_size == 0:
                # Execute batch operation
                write_batch.commit()
                write_batch = WriteBatch(client=self._client)

        if (i + 1) % batch_size != 0:
            # Execute batch operation
            write_batch.commit()

    def _check_restrictions(self, doc: BaseModel, is_update: bool = False):
        # Check for any restrictions
        for key in self.get_unique_keys():
            value = getattr(doc, key)
            try:
                doc_db = self.get_by_attribute(key, value)

                # If the clashing document is itself the allow clash
                if is_update and doc.id == doc_db.id:
                    continue

                raise Conflict(f"{self.name} with {key} {value} already exists")
            except NotFound:
                # No document with given unique key found
                pass

    def _check_restrictions_attributes(self,
                                       doc_id: str,
                                       attributes: Dict[str, Any]):
        # Check for any restrictions
        for key in self.get_unique_keys():
            if key not in attributes:
                continue
            value = attributes.get(key)
            try:
                doc_db = self.get_by_attribute(key, value)

                # If the clashing document is itself then allow clash
                if doc_id == doc_db.id:
                    continue

                raise Conflict(f"{self.name} with {key} {value} already exists")
            except NotFound:
                # No document with given unique key found
                pass

    def _parse_conditions(self, conditions: List[Tuple[str, str, Any]]) -> List[Tuple[str, str, Any]]:
        conditions = list(conditions)
        conditions_parsed = []
        if self.schema_props is not None:
            for attribute, operator, value in conditions:
                attr_props = self.schema_props.get(attribute, {})
                any_of = attr_props.get('anyOf', [])

                # Check if schema is a datetime
                if any((
                    x.get('format') == 'date-time'
                    for x in any_of
                )):
                    if type(value) == str:
                        try:
                            value = DatetimeWithNanoseconds.fromisoformat(value)
                        except ValueError:
                            pass
                conditions_parsed.append((attribute, operator, value))
        return conditions_parsed
