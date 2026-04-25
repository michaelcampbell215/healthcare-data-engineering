import os

from google.cloud import bigquery
from dotenv import load_dotenv

# Migration anchor: validated against source CSV (15,385,047 data rows)
EXPECTED_ROW_COUNT = 15_385_047

# Load environment variables
load_dotenv()5

# Set environment variables
project_id = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project_id)
dataset_id = os.getenv('DATASET_ID')
table_id = os.getenv('TABLE_ID')
full_table_id = f"{project_id}.{dataset_id}.{table_id}"
uri = os.getenv('GCS_URI')


def load_table_from_gcs(client, uri, full_table_id, schema_path, expected_rows):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=client.schema_from_json(schema_path),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True
    )
    load_job = client.load_table_from_uri(uri, full_table_id, job_config=job_config)
    load_job.result()

    assert load_job.output_rows == expected_rows, \
        f"ERROR: Expected {expected_rows:,} rows, loaded {load_job.output_rows}"

    print(f"✓ QA PASSED: {load_job.output_rows:,} rows into {full_table_id} matches expected count")


load_table_from_gcs(client=client, 
    uri=uri, 
    full_table_id=full_table_id, 
    schema_path=os.getenv('SCHEMA_FILE_PATH'), 
    expected_rows=EXPECTED_ROW_COUNT
)