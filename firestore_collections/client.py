import os

from google.cloud.firestore import Client

client = Client(project=os.getenv('PROJECT_ID'))
