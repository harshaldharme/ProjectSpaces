import json
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import io

project_id = "savvy-parser-441207-g9"
dataset_id = "tf_demo_dataset"
table_id = "sample_cust_tbl"

def load_json_to_bigquery(project_id, dataset_id, table_id, json_data):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define schema explicitly (in the desired order)
    schema = [
        bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dob", "TIMESTAMP", mode="NULLABLE"),  # Assuming date-time format, adjust if it's a date
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("username", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("password", "STRING", mode="NULLABLE"),
    ]
    
    # Prepare the LoadJobConfig with autodetect enabled for schema and create_disposition set to CREATE_IF_NEEDED
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,  # Explicit schema ensures order
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    
    # Create the job to load the JSON data
    try:
        # Convert JSON data to newline-delimited format
        json_str = "\n".join(json.dumps(record) for record in json_data)
        json_bytes = json_str.encode("utf-8")

        # Load data from in-memory JSON
        load_job = client.load_table_from_file(
            file_obj=io.BytesIO(json_bytes),
            destination=table_ref,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")

def fetch_random_users():
    url = "https://randomuser.me/api/"
    user_records = []  # List to store user records

    try:
        # Loop to fetch 10 records
        for _ in range(10):
            response = requests.get(url)  # Make GET request to the API
            if response.status_code == 200:
                data = response.json()  # Parse the JSON response
                user_records.append(data['results'][0])  # Add the user record to the list
            else:
                print(f"Error: API call failed with status code {response.status_code}")
                return {"error": "Failed to fetch data"}

        # # Return the results as a JSON string
        # return json.dumps(user_records, indent=4)  # Pretty print with indentation
        return user_records

    except Exception as e:
        # Handle exceptions and return error details
        return json.dumps({"error": str(e)}, indent=4)



# Run the function and print the output
def get_data(event):
    result = fetch_random_users()
    # Extract only the required fields
    extracted_data = []

    for record in result:
        extracted_data.append({
            "first_name": record.get('name', {}).get('first'),
            "last_name": record.get('name', {}).get('last'),
            "dob": record.get('dob', {}).get('date'),
            "gender": record.get('gender'),
            "phone": record.get('phone'),
            "email": record.get('email'),
            "city": record.get('location', {}).get('city'),
            "state": record.get('location', {}).get('state'),
            "country": record.get('location', {}).get('country'),
            "username": record.get('login', {}).get('username'),
            "password": record.get('login', {}).get('password')
        })
    print(extracted_data)
    load_json_to_bigquery(project_id, dataset_id, table_id, extracted_data)
    print(f"Records updated to bigquery table successfully")
    return ""
