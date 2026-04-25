import os
import json
from google.cloud import bigquery
from dotenv import load_dotenv


load_dotenv()

project_id = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project_id)
dataset_id = os.getenv('DATASET_ID')
table_id = os.getenv('TABLE_ID')
full_table_id = f"{project_id}.{dataset_id}.{table_id}"
uri = os.getenv('GCS_URI')



# Job Config
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
    schema= client.schema_from_json(os.getenv('SCHEMA_FILE_PATH')),
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,    
    allow_quoted_newlines=True
)

load_job = client.load_table_from_uri(uri, full_table_id, job_config=job_config)
load_job.result()

print(f"Loaded {load_job.output_rows} rows into {full_table_id}")