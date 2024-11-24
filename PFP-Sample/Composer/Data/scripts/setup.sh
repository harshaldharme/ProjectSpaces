#!/bin/bash

# Check if the correct number of arguments are passed
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <project_id> <dataset_name> <table_name> <schema_json_file>"
    exit 1
fi

# Assign input parameters to variables
PROJECT_ID=$1
DATASET_NAME=$2
TABLE_NAME=$3
SCHEMA_JSON_FILE=$4

# Print a welcome message
echo "Welcome! This script will create a BigQuery dataset and table."

# Step 1: Create BigQuery dataset
echo "Creating dataset $DATASET_NAME in project $PROJECT_ID..."
bq mk --project_id=$PROJECT_ID --dataset $PROJECT_ID:$DATASET_NAME

# Step 2: Create BigQuery table
echo "Creating table $TABLE_NAME in dataset $DATASET_NAME using schema from $SCHEMA_JSON_FILE..."
bq mk --table $PROJECT_ID:$DATASET_NAME.$TABLE_NAME $SCHEMA_JSON_FILE

echo "Dataset $DATASET_NAME and Table $TABLE_NAME created successfully!"
