This page explains how to use the schema, create and populate a bigquery table for the sample pipeline.
Table creation command - 

gcloud bigquery tables create <project_id>:<dataset_id>.<table_name> \
  --schema=schema.json \
  --time_partitioning_type=DAY \
  --time_partitioning_field=bq_ingestion_time \
  --description="Table for storing RandomUser API data"
