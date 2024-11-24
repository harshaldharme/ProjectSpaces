This page explains how to use the schema, create and populate a bigquery table for the sample pipeline.

This is regular text, and this is a subscript: <sub>subscript text</sub>.

Dataset creation - 
bq mk --dataset savvy-parser-441207-g9:pfp_landing_dataset

Table creation - 
bq mk --table \\
--schema ./customer_tbl_schema.json \
--time_partitioning_field bq_ingestion_time \
--time_partitioning_expiration 2592000 \
--expiration 5184000 \
savvy-parser-441207-g9:pfp_landing_dataset.pfp_landing_table

