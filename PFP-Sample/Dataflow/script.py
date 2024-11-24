import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import requests
from google.cloud import bigquery
import json

# Function to make an API call and return a single record
def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data['results'][0]  # Assuming 'results' contains the record
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Function to transform the API data into the BigQuery schema format
def transform_to_bq_format(element):
    # Map the API response fields to BigQuery schema
    return {
        'first_name': element.get('name', {}).get('first', 'N/A'),
        'last_name': element.get('name', {}).get('last', 'N/A'),
        'dob': element.get('dob', {}).get('date', 'N/A'),  # Assuming the 'dob' field contains a date
        'gender': element.get('gender', 'N/A'),
        'phone': element.get('phone', 'N/A'),
        'email': element.get('email', 'N/A'),
        'city': element.get('location', {}).get('city', 'N/A'),
        'state': element.get('location', {}).get('state', 'N/A'),
        'country': element.get('location', {}).get('country', 'N/A'),
        'username': element.get('login', {}).get('username', 'N/A'),
        'password': element.get('login', {}).get('password', 'N/A')
    }

# Function to fetch multiple records from the API
def fetch_data_batch(api_url, num_calls=10):
    return [fetch_data_from_api(api_url) for _ in range(num_calls)]

# Beam pipeline function
def run(api_url, bigquery_table, beam_options):
    # Step 1: Fetch API data in batch (10 records per batch)
    api_data = fetch_data_batch(api_url)
    print(f"Data: {api_data}")
    with beam.Pipeline(options=beam_options) as pipeline:
        
        # Step 2: Create a PCollection from the fetched API data
        records_pcoll = pipeline | 'Create API Data' >> beam.Create(api_data)

        # Step 3: Transform the records into BigQuery format
        transformed_data = records_pcoll | 'Transform Data' >> beam.Map(transform_to_bq_format)

        # # Define the schema for BigQuery
        # schema = [
        #     bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("dob", "TIMESTAMP", mode="NULLABLE"),  # Assuming date-time format, adjust if it's a date
        #     bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("username", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("password", "STRING", mode="NULLABLE")
        # ]

        # Step 4: Write the transformed data to BigQuery
        transformed_data | 'Write To  BigQuery' >> beam.io.WriteToBigQuery(
            bigquery_table,
            # schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        pipeline.run().wait_until_finish()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_url', required=True)
    parser.add_argument('--bigquery_table', required=True)

    known_args, pipeline_args = parser.parse_known_args()

    # Pass pipeline arguments to Beam
    beam_options = PipelineOptions(pipeline_args)

    run(known_args.api_url, known_args.bigquery_table, beam_options)
