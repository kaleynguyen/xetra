The repo hosts a production ETL pipelines that use tools such as Python, Git, Docker, Airflow and 
Kubernetes to automate the ETL jobs from a source s3 bucket to a target s3 bucket on a 
weekly basis. 


# Workflow
1. Host the code repo on github and create a docker image
2. Push the docker image to the docker hub image repo
3. Use Airflow to pull the docker image from the docker repo and handles the workflows orchestration
4. Kubernetes creates a docker container to execute the platform that contains config files, application codes and secrets

Input: Xetra s3 bucket => workflow => Target s3 bucket in parquet format
Requirements: 

Given an input of the first date, extract all the transformed data elements from the first date up until today.
Auto-detection of the source files to be process since each job is expensive
Configurable production-ready Python job to be invariant to the column names of the source bucket 

# Production Environment 
![Production Env](images/production_environment.pdf)