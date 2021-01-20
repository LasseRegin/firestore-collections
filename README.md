
# Firestore Collections

Simple Firestore collection definitions and queries using pydantic schemas and Firestore query API.

A quick and easy way to make use of the NoSQL document database solution [Firestore](https://cloud.google.com/firestore) available in Google Cloud Platform (GCP).

## Requirements

* Python 3.6+
* GCP project with Firestore enabled

## Features

* **Schema definition and validation**: Define collection schemas using `pydantic` and built-in type hinting (`typing`).
* **Automatic IDs**: Automatic ID generation using 12-byte hexadecimal (`bson.ObjectId`).
* **Simple queries**: Query collections using a simple interface.
* **Auxiliary timestamps**: Automatically added timestamps to objects like `created_at` and `updated_at`.

## Example

Define _users_ collection and perform different queries:
```python
from typing import Optional

from pydantic import EmailStr, SecretStr
from firestore_collections import Collection, Schema


class User(Schema):
    __collection_name__ = 'users'
    __unique_keys__ = ['email']

    email: EmailStr
    full_name: str = None
    password: Optional[SecretStr]


# Initialize firestore collection
collection = Collection(schema=User)

# Initialize user object
user = User(
    email='john@doe.com',
    full_name='John')

# Insert
user = collection.insert(user)

# Get object from db
user = collection.get(user.id)

# Update
user.full_name = 'John Doe'
collection.update(user)

# Get by attribute
user = collection.get_by_attribute('email', 'john@doe.com')

# Get all objects
users = collection.get_all()

# Delete object
collection.delete(id=user.id)
```

## GCP credentials

**NOTE**: The package assumes a valid GCP credentials file is available and its path defined in the environment variable `GOOGLE_APPLICATION_CREDENTIALS`.

## License

This project is licensed under the terms of the MIT license.
