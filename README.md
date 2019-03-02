# Purpose

This script creates an example sales order table in pipe-delimited csv and uploads the table to s3. 

The table is partitioned by year and month to enable faster, more cost-effective usage with tools like AWS Athena. 

The S3 files are compressed in GZIP format to save additional storage costs.