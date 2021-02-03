# Quicksight

This project contains the source code for our Data extractions and integrations into AWS Quicksight

The project is build based on a AWS SAM template. This can be found in template.yaml

The application is build based on the following:

- Lambda job to start a step function
- Step function that orchestrates the entire ETL.
- Lambda function that downloads the application data from supplied APIs. Every endpoint gets it own process.
- Raw data is stored on S3 in Parquet format
- Lambda function that processes the raw data and creates it in a tabular format 
    - creating separate files from nested objects
    - fixing column types
    - setting timestamps to UTC
    
- Processed data is stored on S3 in Parquet format. This processed data is aimed to be used for any data ingestion and reporting

# Quicksight
## Chargebee
- Lambda function that Create tables with only the latest known values Chargebee 
    - groupby id and sort by updated_at and give row numbers (RN) . Filter where RN = 1
    - if a subscription is deleted at any time, filter all occurences from data
    - store on S3 as parquet as latest-<table_name>
- Lambda function that kicks off glue crawler to load data into tables
- Lambda function to refresh datasets in Quicksight 
  - NOTE: Adding data needs to be done manually and ID added to step function parameters
    
S3 Data Lake structure
Buckets
- LANDING
  - APPLICATION
    - ENDPOINT
      - PARTITION
    example: landing_bucket/chargebee/subscriptions/eu/
- RAW
  - APPLICATION
    - TABLE
        - PARTITION
    example: raw_bucket/chargebee/addons/eu/ 
- PROCESSED
    - TYPE
        - TABLE
    example: processed_bucket/latest/latest_subscriptions/
          
* Landing - a temporary store with data extracted from the source systems.
* Raw - This is the primary bucket which contains the primary ‘data lake’. 
  If source data has nested lists, these are processed and transformed into individual tables here.
* Processed - This is the latest snapshot of raw data that has had transformations applied. 
  Transformations can include basic views, restructuring the data, joins of tables. 
  Any ad-hoc reporting is done in this stage. These tables can be loaded into analytics platforms like quicksight


