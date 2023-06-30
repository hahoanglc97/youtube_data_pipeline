# Youtube data pipeline with AWS and Apache Airflow
## Hand on with API, Amazon cloud services and Airflow
<hr/>

- The goal of the project is to create a data pipeline that collects YouTube data. This pipeline should collect data on a daily basis in order to keep the information up to date
- The data collected with the pipeline will be used to answer the following questions:
    + What are the most popular video categories in each country?
    + Does the most popular video category change over time? If so, how?
### I. Tools
- **Youtube API** for crawl data
- **Apache Airflow** for schedule and execute task
- **Amazon EC2** implement Apache Airflow
- **Amazon S3** storage data
- **Amazon Athena** for query
- boto3 libs for interactive with S3 and Athena services

### II. Architecture
![alt Architecture](/Architecture.png)

### III. How it works?
1. Task is written by Python. Task will execute:
    + Get data via Youtube API
    + Extract and transform this data - get and merge some data useful
    + Load data stored in S3 service
    + Excute query on Athena service. It will get categories have most video uploads of each region and save result in S3 service
2. Task will be scheduled by Apache Airflow run daily.
3. Airflow is implemented by EC2 service
4. Athena will execute query on S3 for get categories have most video uploads
5. S3 service store data crawl from Youtube and store result query of Athena service

#### Airflow dags graph
![alt dags graph](/airflow-dags.png)



[Link](https://towardsdatascience.com/creating-a-youtube-data-pipeline-with-aws-and-apache-airflow-e5d3b11de9c2)