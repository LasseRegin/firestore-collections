from typing import Optional
import unittest

from pydantic import EmailStr, SecretStr
from firestore_collections import Schema, Collection
from google.cloud.firestore_v1.collection import CollectionReference


class TestSchema(Schema):
    __collection_name__ = 'test'

    email: EmailStr
    full_name: str = None
    password: Optional[SecretStr]


class TestCollection(unittest.TestCase):
    def test_collection_init(self):
        collection = Collection(schema=TestSchema)
        self.assertIsInstance(collection, Collection)
        self.assertIsInstance(collection.collection, CollectionReference)

    def test_collection_query(self):
        collection = Collection(schema=TestSchema)

        # Initialize test object
        obj = TestSchema(email='john@doe.com', full_name='John')
        self.assertIsInstance(obj, TestSchema)

        # Insert
        obj_db = collection.insert(obj)
        obj_id = obj_db.id
        self.assertIsInstance(obj_db, TestSchema)

        # Get object
        obj_db = collection.get(obj_id)
        self.assertIsInstance(obj_db, TestSchema)
        self.assertEqual(obj_db.id, obj_id)

        # Update
        obj_db.full_name = 'John Doe'
        collection.update(obj_db)

        # Get by attribute
        obj_db = collection.get_by_attribute('email', 'john@doe.com')
        self.assertIsInstance(obj_db, TestSchema)
        self.assertEqual(obj_db.full_name, 'John Doe')

        # Get all objects
        objects = collection.get_all()
        self.assertIsInstance(objects, list)
        self.assertEqual(len(objects), 1)

        # Delete object(s)
        for obj in objects:
            collection.delete(id=obj.id)


if __name__ == '__main__':
    unittest.main()