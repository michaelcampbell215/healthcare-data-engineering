import os

from dotenv import load_dotenv
from google.cloud import storage


# CONFIGURATION
load_dotenv()


project_id = os.getenv('GCP_PROJECT_ID')
bucket_name = os.getenv('GCS_BUCKET_NAME')
file_path = os.getenv('CSV_FILE_PATH')

destination_blob_name = f"raw/raw_general_payments.csv"

def upload_blob(bucket_name, file_path, destination_blob_name, project_id):
    """
    Upload a file to a GCS bucket.
    Args:
    bucket_name: The name of the GCS bucket.
    file_path: The path to the file to upload.
    destination_blob_name: The name of the file in the GCS bucket.
    """

    try:
        # Initialize the GCS client
        client = storage.Client(project_id)
        # Get the bucket
        bucket = client.get_bucket(bucket_name)
        # Get the blob
        blob = bucket.blob(destination_blob_name)
        blob.chunk_size = 5 * 1024 * 1024

        #Upload the local file
        blob.upload_from_filename(file_path, timeout=600)

        print("Success: File uploaded!")
    except Exception as e:
        print(f"Error: {e}")
        raise

    
upload_blob(bucket_name, file_path, destination_blob_name, project_id)
  
