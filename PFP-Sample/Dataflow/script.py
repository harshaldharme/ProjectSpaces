import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import requests
from google.cloud import bigquery
import json
import datetime
import os

# Function to make an API call and return a single record
def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        json_data = data['results'][0]
        json_data['bq_ingestion_time'] = datetime.datetime.utcnow().isoformat()
        return json_data  # Assuming 'results' contains the record
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Function to transform the API data into the BigQuery schema format
def transform_to_bq_format(json_data):
    # Map the API response fields to BigQuery schema
    # Flatten the nested fields into a BigQuery-friendly format
    return {
        'gender': json_data.get('gender', 'N/A'),
        'title': json_data.get('name', {}).get('title', 'N/A'),
        'first_name': json_data.get('name', {}).get('first', 'N/A'),
        'last_name': json_data.get('name', {}).get('last', 'N/A'),
        'street_number': json_data.get('location', {}).get('street', {}).get('number', 'N/A'),
        'street_name': json_data.get('location', {}).get('street', {}).get('name', 'N/A'),
        'city': json_data.get('location', {}).get('city', 'N/A'),
        'state': json_data.get('location', {}).get('state', 'N/A'),
        'country': json_data.get('location', {}).get('country', 'N/A'),
        'postcode': json_data.get('location', {}).get('postcode', 'N/A'),
        'latitude': json_data.get('location', {}).get('coordinates', {}).get('latitude', 'N/A'),
        'longitude': json_data.get('location', {}).get('coordinates', {}).get('longitude', 'N/A'),
        'email': json_data.get('email', 'N/A'),
        'username': json_data.get('login', {}).get('username', 'N/A'),
        'uuid': json_data.get('login', {}).get('uuid', 'N/A'),
        'dob_date': json_data.get('dob', {}).get('date', 'N/A'),
        'dob_age': json_data.get('dob', {}).get('age', 'N/A'),
        'registered_date': json_data.get('registered', {}).get('date', 'N/A'),
        'registered_age': json_data.get('registered', {}).get('age', 'N/A'),
        'phone': json_data.get('phone', 'N/A'),
        'cell': json_data.get('cell', 'N/A'),
        'id_name': json_data.get('id', {}).get('name', 'N/A'),
        'id_value': json_data.get('id', {}).get('value', 'N/A'),
        'nat': json_data.get('nat', 'N/A'),
        'bq_ingestion_time': json_data['bq_ingestion_time']
        }

# Function to fetch multiple records from the API
def fetch_data_batch(api_url, num_calls=100):
    return [fetch_data_from_api(api_url) for _ in range(num_calls)]

# Function to read schema from a JSON file
def load_schema_from_file(file_path):
    fullpath = f'{os.getcwd()}/{file_path}'
    with open(fullpath, 'r') as file:
        schema = json.load(file)
    return schema

# Beam pipeline function
def run(api_url, bigquery_table, beam_options):
    # Step 1: Fetch API data in batch (100 records per batch)
    api_data = fetch_data_batch(api_url)
    print(f"Data: {api_data}")

    with beam.Pipeline(options=beam_options) as pipeline:
        
        # Step 2: Create a PCollection from the fetched API data
        records_pcoll = pipeline | 'Create API Data' >> beam.Create(api_data)

        # Step 3: Transform the records into BigQuery format
        transformed_data = records_pcoll | 'Transform Data' >> beam.Map(transform_to_bq_format)

        # Step 4: Write the transformed data to BigQuery
        transformed_data | 'Write To  BigQuery' >> WriteToBigQuery(
            bigquery_table,
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
