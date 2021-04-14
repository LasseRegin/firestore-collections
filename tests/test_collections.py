from typing import Optional
import unittest

from pydantic import EmailStr, SecretStr
from firestore_collections import Schema, Collection, NotFound
from google.cloud.firestore_v1.collection import CollectionReference


class TestSchema(Schema):
    __collection_name__ = 'test'
    __unique_keys__ = ['email']

    email: EmailStr
    full_name: str = None
    address: str = None
    password: Optional[SecretStr]


class TestCollection(unittest.TestCase):
    def test_collection_init(self):
        collection = Collection(schema=TestSchema)
        self.assertIsInstance(collection, Collection)
        self.assertIsInstance(collection.collection, CollectionReference)

    def test_collection_query(self):
        collection = Collection(schema=TestSchema)

        # Initialize test object
        email = 'john@doe.com'
        obj = TestSchema(email=email, full_name='John')
        self.assertIsInstance(obj, TestSchema)

        try:
            obj_db = collection.get_by_attribute('email', email)
            collection.delete(id=obj_db.id)
        except NotFound:
            pass

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

        # Update a single attribute
        new_full_name = 'John Doe Jr.'
        collection.update_attribute(
            doc_id=obj_db.id,
            attribute='full_name',
            value=new_full_name)
        obj_db = collection.get_by_attribute('email', 'john@doe.com')
        self.assertEqual(obj_db.full_name, new_full_name)
        self.assertEqual(obj_db.email, 'john@doe.com')

        # Update a set of attributes
        new_full_name = 'John Doe Jr. the 2nd'
        new_address = 'John Doe Street'
        collection.update_attributes(
            doc_id=obj_db.id,
            attributes={
                'full_name': new_full_name,
                'address': new_address,
            })
        obj_db = collection.get_by_attribute('email', 'john@doe.com')
        self.assertEqual(obj_db.full_name, new_full_name)
        self.assertEqual(obj_db.address, new_address)
        self.assertEqual(obj_db.email, 'john@doe.com')

        # Get by attribute
        obj_db = collection.get_by_attribute('email', 'john@doe.com')
        self.assertIsInstance(obj_db, TestSchema)
        self.assertEqual(obj_db.full_name, new_full_name)

        # Get all objects
        objects = collection.get_all()
        self.assertIsInstance(objects, list)
        self.assertEqual(len(objects), 1)

        # Delete object(s)
        for obj in objects:
            collection.delete(id=obj.id)


if __name__ == '__main__':
    unittest.main()