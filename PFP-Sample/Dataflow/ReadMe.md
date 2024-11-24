This page explains how to run the dataflow job for the script.py

Command - 
python script.py --api_url='https://randomuser.me/api/' --bigquery_table='savvy-parser-441207-g9.pfp_landing_dataset.pfp_landing_table' --runner=DataflowRunner --project='savvy-parser-441207-g9' --region='us-east1' --service_account_email='pfp-dataflow-sa@savvy-parser-441207-g9.iam.gserviceaccount.com' --temp_location='gs://dataflow-staging-bucket-108532284999/temp'
