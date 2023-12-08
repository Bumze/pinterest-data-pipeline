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

This is a final project from the AIcore Data engineering programme. The project serves to equip one with essential skills in industry standard tools for building data pipelines. It fills the need to get hands on some infrastructure similar to that which a data engineer would hb find when working at Pinterest. 

## Project Brief

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, a similar system is created using the AWS Cloud infrastructure.

## Project Requirements

- Git hub
- Amazon Web Services. AWS services used in the project are:
- AWS EC2 
- AWS S3
- AWS MSK
- AWS API Gateway
- AWS Managed Apache Airflow
- AWS Kinesis
- Python files - Two Pinterest events emulators, one for batch processing, written to populate event data in batches from an AWS RDS server and the other for streaming data processing.
- PySpark
PySpark has many dependencies, not only with other Python packages, but also with other modules that are not easily installed using the convenient pip install command. Follow the next steps:

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

It is important to set the directory as SPARK_HOME, otherwise, PySpark won't know where to find the corresponding commands. To do so, set it as an environment variable copying the following command in your ~/.bashrc file:
export SPARK_HOME=<path to your home directory>/spark/spark-3.1.2-bin-hadoop3.2

Note: The command above depends on where you extracted the files you downloaded and the version
Don't skip this step. Having an incorrectly set SPARK_HOME environment variable is the cause of many common issues with Spark
Save your ~/.bashrc. You should be able to use PySpark now! If not, try restarting vscode, then try restarting your computer if that doesn't work.
To check if the installation was successful, you can install findspark (pip install findspark) and run the following cell
- Install Java
- Get a Databricks account 

## Main 

Set up the required AWS account, python file, and git hub.
  - Navigate to https://aws.amazon.com/  to sign in to the AWS Console.
    Log in with the following credentials:
  - Account ID: xxxxxxxxxxxx
  - IAM user name: xxxxxxxxxxx
  - Password: xxxxxxxxxxxxx

Caution- Make sure to check you are in the correct region when using a new service while conducting the project
  
# Set up a EC2 client machine locally.  
Step 1: Create a .pen file locally.
This is a key pair file which ends in the .pem extension. This file will allow a connection to the EC2 instance. To do this,
 - Navigate to Parameter Store in your AWS account.
 - Using your KeyPairId (you can locate this information within the email containing your AWS login credentials) find the specific key pair associated with your EC2 instance. Select this key pair and under the Value field select Show.This will reveal the content of your key pair. Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.

Step 2:
Navigate to the EC2 console and identify the instance with your unique UserId. Select this instance, and under the Details section find the Key pair name and make a note of this. Save the previously created file in the VSCode using the following format: Key pair name.pem.


#  Connect to your EC2 instance.

Follow the `Connect` instructions `(SSH client)` on the EC2 console to do this.
Your AWS account has been provided with access to an IAM authenticated MSK cluster. You don't have to create your own cluster for this project.
In order to connect to the IAM authenticated cluster, you will need to install the appropriate packages on your EC2 client machine.

# Set up Kafka on the EC2 instance
Step 1:
Install Kafka on your client EC2 machine. Set up the security rules for the EC2 instance to allow communication with the MSK cluster.  Make sure to install the same version of Kafka as the one the cluster is running on (in this case 2.12-2.8.1), otherwise there will not be a communication with the MSK cluster.


Step 2:
Install the IAM MSK authentication package on your client EC2 machine. This package is necessary to connect to MSK clusters that require IAM authentication, like the one you have been granted access to.


Step 3:
Before you are ready to configure your EC2 client to use AWS IAM for cluster authentication, you will need to:

Navigate to the IAM console on your AWS account
Here, on the left hand side select the Roles section
You should see a list of roles, select the one with the following format: <your_UserId>-ec2-access-role
Copy this role ARN and make a note of it, as we will be using it later for the cluster authentication
Go to the Trust relationships tab and select Edit trust policy
Click on the Add a principal button and select IAM roles as the Principal type
Replace ARN with the <your_UserId>-ec2-access-role ARN you have just copied
By following the steps above you will be able to now assume the <your_UserId>-ec2-access-role, which contains the necessary permissions to authenticate to the MSK cluster.


Step 4:
Configure your Kafka client to use AWS IAM authentication to the cluster. To do this, you will need to modify the client.properties file, inside the kafka_folder/bin directory accordingly.


# Create Kafka topics
Step 1:
To create a topic, you will first need to retrieve some information about the MSK cluster, specifically: the Bootstrap servers string and the Plaintext Apache Zookeeper connection string. Make a note of these strings, as you will need them in the next step.


You will have to retrieve them using the MSK Management Console, as for this project you have not been provided with login credentials for the AWS CLI, so you will not be able to retrieve this information using the CLI.


Step 2:
Create the following three topics:

<your_UserId>.pin for the Pinterest posts data
<your_UserId>.geo for the post geolocation data
<your_UserId>.user for the post user data
Before running any Kafka commands, remember to make sure your CLASSPATH environment variable is set properly.


In the create topic Kafka command replace the BootstrapServerString with the value you have obtained in the previous step.

 Create topics with the exact names specified on the MSK cluster. Follow the format, otherwise you will run into permission errors.


- Building the pipeline
    - Create an Apache cluster using AWS MSK
    - Create a client machine for the cluster
    - Enable client machine to connect to the cluster
    - Install Kafka on the client machine
    - Create topics on the Kafka cluster. Three topics were created, a pin 
    - Deliver messages to the Kafka cluster using the API gateway


## Batch processing

An API is built in Amazon API Gateway that will send data to an S3 bucket. Then Amazon MSK Connect is used to connect MSK clusters to the S3 bucket so that data fromm the custers is saved in the S3 bucket. 

Steps:


- Create a S3 bucket, an IAM role that allows you to write to the bucket or a VPC Endpoint to S3.
- Go to the S3 console and select your bucket. Make a note of the bucket name, as you will need it in the other steps.
- On your EC2 client, download the Confluent.io Amazon S3 Connector and copy it to your S3 bucket.
- Create a custom plugin in the MSK Connect console. For this project my AWS account only has permissions to create a custom plugin with the following name: <my_UserId>-plugin. 
- Create a connector with MSK Connect. 


When building the connector, make sure to choose the IAM role used for authentication to the MSK cluster in the Access permissions tab. After building the plugin-connector pair, data passing through the IAM authenticated cluster, will be automatically stored in the designated S3 bucket.


## Build an API and a PROXY integration for your API.


Create an API. The API name in this project is my UserId. The next step is to build a PROXY integration for your API.
On Amazon API Gateway,
- Create a resource that allows you to build a PROXY integration for your API.
- Create a HTTP ANY method. When setting up the Endpoint URL, copy the correct PublicDNS from the EC2 machine.
- Deploy the API and make a note of the Invoke URL, you will need it in a later task.


 ## Set up the Kafka REST Proxy integration for your API, set up the Kafka REST Proxy on your EC2 client machine.


- First, download and install the Confluent package for the Kafka REST Proxy on your EC2 client machine.


- Allow the REST proxy to perform IAM authentication to the MSK cluster by modifying the kafka-rest.properties file.


- Start the REST proxy on the EC2 client machine.


 # Send data to your API
 
 Sending data to the API will send the data to the MSK Cluster using the plugin-connector pair previously created.


Step 1:

Use the user_posting_emulation.py to send data to your Kafka topics using your API Invoke URL. Data will be sent from the three tables to their corresponding Kafka topics.


Step 2:

Check data is sent to the cluster by running a Kafka consumer (one per topic). If everything has been set up correctly, you should see messages being consumed.


Step 3:

Check if data is getting stored in the S3 bucket. The folder organization here will be topics/<your_UserId>.pin/partition=0/ that your connector creates in the bucket.



## Batch processing with Databricks


Databricks is an integrated analytics environment powered by Apache Spark which lets you connect and read from many data sources such as AWS S3, HDFS, MySQL, Cassandra etc. In this project, data is read from an Amazon S3 bucket.


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


##  Batch processing: Spark on Databricks


For data to be processed in batches on Databricks, one must mount the S3 bucket on to Databricks filestore. The steps are detailed in the Databricks(data_from_S3_and_query).ipynb file. 


The steps are as follows:


# Mount S3 bucket to Databricks

 - Open a notebook in the Databricks UI, 
 - Select the New icon and then select Notebook. Codes can be written here.
 - Check the contents in FileStore, the location where we uploaded the AWS credentials in the last step, by running the following command:
 

 "dbutils.fs.ls(“/FileStore/tables”)"


 The CSV file uploaded earlier should now be inside the FileStore tables folder.
 

 - Import the pyspark functions and URL processing libraries 
 - Mount the bucket to Databricks filestore.
 - See Databricks(data_from_S3_and_query).ipynb file for details

Successful mounting of the bucket can be tested and the bucket is mounted only once. Once mounted it is accessible  from Databricks at any time.





Import necessary libraries
List tables in Databricks filestore in order to obtain AWS credentials file name
Read the credentials .csv into a Spark dataframe
Generate credential variables from Spark dataframe
Mount the S3 bucket containing the messages from the Kafka topics
List the topics
Read the .json message files into three Spark dataframes, one each for each of the topics
Unmount the S3 bucket

