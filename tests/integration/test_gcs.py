from src.utils.gcs import upload_to_gcs, delete_object
from dotenv import load_dotenv
from os import environ
from google.cloud import storage
from datetime import datetime

import random
import string


# Prepare database credentials
load_dotenv('.env')

GCP_PROJECT_NAME = environ.get('GCP_PROJECT_NAME')
GCP_CREDENTIALS_FILE = environ.get('GCP_CREDENTIALS_FILE')
GCS_BUCKET_NAME = environ.get('GCS_BUCKET_NAME')

UPLOAD_FILE_PATH = 'tests/integration/sample_text_file.txt'


def test_upload_file():
    
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client.from_service_account_json(GCP_CREDENTIALS_FILE, project=GCP_PROJECT_NAME)

    # Get the bucket object
    bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
    
    # Generate unique object key suffix (for uniqueness)
    timestamp_chars = datetime.now().strftime('%Y%m%dT%H%M%S')
    random_chars = ''.join(random.choice(string.ascii_letters) for _ in range(8))
    object_key = f'test_upload__{timestamp_chars}__{random_chars}.txt'

    # Upload file to GCS bucket
    upload_to_gcs(UPLOAD_FILE_PATH, GCS_BUCKET_NAME, object_key)
    
    # Verify that object exists in bucket
    try:
        assert object_key in [o.name for o in bucket.list_blobs()], 'File not found in GCS bucket.'
        delete_object(GCS_BUCKET_NAME, object_key)
        
    except Exception as e:
        delete_object(GCS_BUCKET_NAME, object_key)  # Always clean up.
        raise e
        

def test_delete_object():
    
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client.from_service_account_json(GCP_CREDENTIALS_FILE, project=GCP_PROJECT_NAME)

    # Get the bucket object
    bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
    
    # Generate unique object key suffix (for uniqueness)
    timestamp_chars = datetime.now().strftime('%Y%m%dT%H%M%S')
    random_chars = ''.join(random.choice(string.ascii_letters) for _ in range(8))
    object_key = f'test_upload__{timestamp_chars}__{random_chars}.txt'

    # Upload file to GCS bucket
    upload_to_gcs(UPLOAD_FILE_PATH, GCS_BUCKET_NAME, object_key)
    
    # Verify that object was uploaded successfully
    assert object_key in [o.name for o in bucket.list_blobs()], 'File not found in GCS bucket.'
    
    # Delete object
    delete_object(GCS_BUCKET_NAME, object_key)
    
    # Verify that object was deleted successfully
    assert object_key not in [o.name for o in bucket.list_blobs()], 'File still exists in GCS bucket.'
    