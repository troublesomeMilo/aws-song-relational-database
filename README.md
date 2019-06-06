## Data Modeling with Amazon Redshift and AWS Resources - Song Play Database

By Harley 

### Objective

The objective of this repository is to show how AWS resources can be use to build and use a database in the AWS environment. The input data comes from 1) a subet of a database of songs and 2) a simulated song play log. Both are stored on an AWS S3 bucket. The scripts in this repository will perform the following actions:

1. Connects to a Redshift cluster to create tables
2. Extract data files in the S3 bucket and loads it into staging tables
3. Transform data when applicable
4. Load data into proper tables of the star schema

### To Run The Program

To run the program, two steps must be taken in the AWS management console: 1) A redshift cluster must be spun up and 2) a redshift IAM role allowing read access to S3 must be created. The `dwh_template.cfg` file must be modified to include the following: 1) the redshift cluster information must be added under the `CLUSTER` heading, 2) the IAM role ARN must be added under the `IAM_ROLE` heading, and 3) the config file must be renamed to `dwh.cfg`. 

Once complete, the `create_tables.py` script can be run to create the tables and the `etl.py` script can be run to load the tables with data. 

### Inputs

### Inputs

Raw data for this project comes from two sources, a database of song information and a simulated log of song plays. Both datasets are stored in an S3 bucket. See the data file urls stored in the `dwh_template.cfg` file.

### Repository Files

`dwh_template.cfg`

This config file stores three sets of key inputs: 1) Information about the redshift cluster, 2) the ARN for the IAM role needed to read the S3 bucket, and 3) the urls and filepaths of the data files in the S3 bucket. This file is incomplete and must be renamed to use properly. See the To Run The Program section for more information.

`sql_queries.py`

This script is a repository of sql queries, stored in variables, that are subsequently imported into both the `create_tables.py` and `etl.py` scripts for use with the redshift database created. they include queries to 1) drop existing tables, 2) create appropriate tables, and 3) insert data into tables.

`create_tables.py`

This script performs several steps in the process of generating the tables. The `psycopg2` library is used to interface with redshift. Queries to be executed are imported from `sql_queries.py`. The steps are generally as follows:

1. The config file is parsed and read
2. A connection is made to the cluster specified in the config file
3. A cursor object is generated to perform action on the cluster
4. The drop table queries, imported from `sql_queries.py`, are executed
5. The create table queries, imported from `sql_queries.py`, are executed
6. The connection is closed

`etl.py`

This script performs several steps in the process of populating the database with data. The `psycopg2` library is used to interface with AWS resources. Queries to be executed are imported from `sql_queries.py`. The steps are generally as follows:

1. The config file is parsed and read
2. A connection is made to the cluster specified in the config file
3. A cursor object is generated to perform action on the cluster
4. The staging tables are loaded with data using S3 bucket and IAM role information specified in the config file using queries imported from `sql_queries.py`
5. The final end-user tables are loaded with data from the staging tables using queries imported from `sql_queries.py`
6. The connection is closed

