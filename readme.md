Spotify serverless data engineering pipeline

## Project Overview :
This project demonstrates a serverless data engineering pipeline that extracts music data from the Spotify API, transforms it using AWS Lambda, and enables analytics using Amazon Athena.

The pipeline follows a modern ETL architecture built on AWS managed services, ensuring scalability, reliability, and low operational overhead.

This project implements a complete Extract–Transform–Load (ETL) pipeline using AWS managed services

## Architecture Overview:

Extract:

1. Python application fetches data from the Spotify API

2. Triggered via AWS Lambda

3. Logs monitored using Amazon CloudWatch

Transform:

1. Lambda cleans, normalizes, and converts raw JSON into structured CSV

2. Transformed data stored in Amazon S3

Load & Query:

1. AWS Glue Crawler automatically infers schema

2. Metadata stored in AWS Glue Data Catalog

3. Data queried using Amazon Athena (SQL on S3)

## Key Learnings:

1. Designing serverless data pipelines

2. Working with APIs at scale

3. Schema evolution & data cataloging

4. Cost-effective analytics using Athena

5. This project reflects real-world data engineering workflows used in production systems.

## Architecture:
Extract → Transform → Load

- Extract: Spotify API + Python + AWS Lambda  
- Transform: AWS Lambda (JSON → CSV processing)  
- Load: Amazon S3 → AWS Glue → Amazon Athena 

## Technologies Used:
- Python  
- Spotify Web API  
- AWS Lambda  
- Amazon S3  
- AWS Glue Crawler  
- AWS Glue Data Catalog  
- Amazon Athena  
- Amazon CloudWatch  

## Data Flow :
1. Spotify API provides raw music data (tracks, artists, albums)  
2. Python Lambda function fetches and stores raw JSON in S3  
3. Transformation Lambda cleans and structures data into CSV  
4. AWS Glue Crawler scans S3 and updates the schema automatically  
5. Amazon Athena enables SQL queries on the data  

## Features:

1. Fully serverless architecture

2. Automated schema discovery

3. SQL-based analytics without data movement

4. Scalable and cost-efficient

## Future Enhancements:

1. Convert pipeline to real-time streaming using Kafka/Kinesis

2. Add incremental data loading

3. Store analytics-ready data in Snowflake / Redshift

4. Build dashboards using Amazon QuickSight

Author:

MADA Bharath Reddy
Aspiring Data Engineer
