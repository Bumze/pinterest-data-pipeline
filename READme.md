## Pinterest data pipeline

# Table of contents

- Introduction
- Project Brief
- Project Dependencies
- Installation instructions
- Usage instructions
- File structure of the project 
- License information

## Introducttion

This is a final project from the AIcore Data engineering programme. The project serves to equip one with essential skills in industry standard tools for building data pipelines. It fills the need to get hands on some infrastructure similar to that which data engineer findwhen working at Pinterest. Tools like Amazon AWS, Apache Kafka,... 

## Project Brief

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, a similar system is created using the AWS Cloud.

## Project Requirements

Git hub
Amazon AWS
Python file -Pinterest Events Emulator
(A python script written to populate event data in batches from an AWS RDS server that was set up by Aicore.)
PySpark
PySpark has many dependencies, not only with other Python packages, but also with other modules that are not easily installed using the convenient pip install command. While you can install pyspark using pip install pyspark this is probably not going to be enough. Therefore, we recommend you to follow the next steps:

Visit PySpark download page https://spark.apache.org/downloads.html and:
- Choose latest release
- Download package locally
- Create a folder (for example spark) in a directory that you know will be safe. ~/ is usually a good option.
Extract the files from the downloaded file into the created folder. At the time of writing, the last version was Spark 3.1.2, so, in that case, your directory will look like this (in case you are using the same examples):
~/
│
├── spark/
│   └── spark-3.1.2-bin-hadoop3.2  <--- SPARK_HOME
│         ├── bin
│         ├── conf
│         ├── data
... 
It is important you set the directory as SPARK_HOME, otherwise, PySpark won't know where to find the corresponding commands. To do so, simply set it as a environment variable copying the following command in your ~/.bashrc file:
export SPARK_HOME=<path to your home directory>/spark/spark-3.1.2-bin-hadoop3.2

Note: The command above depends on where you extracted the files you downloaded and the version
Don't skip this step. Having an incorrectly set SPARK_HOME environment variable is the cause of many common issues with Spark
Save your ~/.bashrc. You should be able to use PySpark now! If not, try restarting vscode, then try restarting your computer if that doesn't work.
To check if the installation was successful, you can install findspark (pip install findspark) and run the following cell
- Install Java


## Main 

- Set up the required AWS account, python file, git hub, 
  - Navigate to https://aws.amazon.com/  to sign in to the AWS Console.
    Log in with the following credentials:
  - Account ID: xxxxxxxxxxxx
  - IAM user name: xxxxxxxxxxx
  - Password: xxxxxxxxxxxxx

Caution- Make sure to check you are in the correct region when using a new service while conducting the project
  # Set up a EC2 client machine locally.  
  - Create a .pen file locally
This is a key pair file which ends in the .pem extension. This file will allow a connection to the EC2 instance. To do this, first navigate to Parameter Store in your AWS account.

'Key pair creation process....'

Using your KeyPairId associated with your EC2 instance. 
  - Select this key pair and under the Value field select Show.This will reveal the content of your key pair. Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.

  - Navigate to the EC2 console and select the instance with your unique UserId. Under the Details section find the Key pair name and make a note of this. Save the previously created file in the VSCode using a .pem format.

#  Connect to your EC2 instance.

Create 
- Building the pipeline
    - Create an Apache cluster using AWS MSK
    - Create a client machine for the cluster
    - Enable client machine to connect to the cluster
    - Install Kafka on the client machine
    - Create topics on the Kafka cluster
    - Delivering messages to the Kafka cluster
    - Sending messages to the cluster using the API gateway

.
.
.
.
.

## Databricks
Databricks is an integrated analytics environment powered by Apache Spark which lets you connect and read from many data sources such as AWS S3, HDFS, MySQL, Cassandra etc. In this project, data  is read from an Amazon S3 bucket.

# Create an access key and a secret access key for Databricks in AWS

  - Access the IAM console in your AWS account. 
  - In the IAM console, under Access management click on Users.  
  - Click on the Add users button.
  - Enter the desired User name and click Next.
  - On the permission page, select the Attach existing policies directly choice. In the search bar type AmazonS3FullAccess and check the box. (This will allow full access to S3, meaning Databricks will be able to connect to any existing buckets on the AWS account.)
  - Skip the next sections until you reach the Review page. Here select the Create user button.
  - In the Security Credentials tab select Create Access Key
  - On the subsequent page select Command Line Interface (CLI), navigate to the bottom of the page click I understand.
  - On the next page, give the keypair a description and select Create Access Key.
  - Click the Download.csv file button to download the credentials you have just created. 
  After creating the IAM User, assign it a programmatic access key:
  - In the Security Credentials tab select Create Access Key
  - On the subsequent page select Command Line Interface (CLI), navigate to the bottom of the page click I understand
  - On the next page, give the keypair a description and select Create Access Key
  - Click the Download.csv file button to download the credentials you have just created.

## Upload credential csv file to Databricks
In the Databricks UI, 
 - Click the Data icon and then click Create Table.
 - Click on Drop files to upload, or click to browse and select the credentials file you have just downloaded from AWS. Once the file has been successfully uploaded, you should see a green checkmark next to it.
 The credentials will be uploaded in the following location: /FileStore/tables.

# Mount S3 bucket to Databricks

 - Open a notebook in the Databricks UI, 
 - Select the New icon and then select Notebook. Codes can be written here.
 - Check the contents in FileStore, the location where we uploaded the AWS credentials in the last step, by running the following command:
 
 "dbutils.fs.ls(“/FileStore/tables”)"
 The CSV file uploaded earlier should now be inside the FileStore tables folder.
 
 - Import the pyspark functions and URL processing libraries 
 - Mount the bucket to Databricks
 - See mount_and_read_from_bucket.ipynb for details

Successful mounting of the bucket can be tested while the bucket need to be mounted only once. One hould be able to access it from Databricks at any time.

