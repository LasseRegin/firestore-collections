import os

from firebase_admin import credentials, firestore, initialize_app


# Use the application default credentials
cred = credentials.ApplicationDefault()
app = initialize_app(
    credential=cred,
    options={
        'projectId': os.getenv('PROJECT_ID')
    }
)

client = firestore.client(app=app)
