---
title: "Scalable Model Pipelines"
output: html_document
---

The repo hosts a production ETL pipelines that use tools such as Python, Git, Docker, Airflow and Kubernetes to automate the ETL jobs from a source s3 bucket to a target s3 bucket on a weekly basis. 

# Production Environment Steps
## 1. Set up virtual environment

It is advisable to activate the virtual environment and then pip install packages to eliminate the package dependency issues. 

which python --Windows

where python

pip install pipenv

cd project

pipenv shell --python 3.9 

pipenv shell --python 3.8.10 (more stable)

pip list

pip install pandas numpy awscli jupyter boto3 

pipenv --venv

* Exit and Enter the virtualenv

exit #deactivate the current virtual env

pipenv shell #activate the current virtual env



## 2. Set up AWS

aws s3 ls s3://deutsche-boerse-xetra-pds/2021-10-27 --recursive --no-sign-request

aws configure

aws s3 ls

> Notice that all of the Bash shell commands can be accessed with a notebook by adding ! in the front through SSH connection. We can also access the aws resources through the AWS cli provided by AWS.

## 3. Quick and dirty approach using Jupyter Notebook

## 4. Functional versus OOP
## 5. Testing
## 6. Functional Approach with the quick and dirty
## 8. OOP design + config + metadata + logging
## 9. OOP code design
## 10. Dev env (Github, Python project, VSC)
## 11. Implement class frame
## 12. Implement logging
## 13. Coding (clean code, functionality, linting, unit tests, integration tests)
## 14. Set up dependency using pipenv
## 15. Performance tuning with profiling and timing
## 16. Create dockerfile + push docker image to docker hub
## 17. Run application in production using a minikube and argo workflows / airflow


# Workflow
1. Host the code repo on github and create a docker image
2. Push the docker image to the docker hub image repo
3. Use Airflow to pull the docker image from the docker repo and handles the workflows orchestration.
4. Kubernetes creates a docker container to execute the platform that contains config files, application codes and secrets

Input: Xetra s3 bucket (public) => workflow => Target s3 bucket in parquet format
Requirements: 

Given an input of the first date, extract all the transformed data elements from the first date up until today.

Transformed data requirements: Given a set of ISIN (unique name of the security) and Date, extract the opening price, closing price and percent change of each security daily.

Auto-detection of the source files to be process since each job is expensive

Configurable production-ready Python job to be invariant to the column names of the source bucket 

# Production Environment
![pe](./images/production_environment.pdf)

# Architecture Design Layer

Infrastructure (DB, roots, caches)
-> Adapter (accessing to infrastructure and external API)
-> Application (features of the applications )
-> Domain

# Functional Principles

# OOP Principles
* Composition over inheritance: try to write classes to implement functions instead of looking for super close relationship
* Encapsulate what varies to ensure the independency of each component
* Dependency Inversion Principle: we should not care about WHAT and WHEN so much and decouple the code as much as possible.

# Logging
It gives logs to stdout, save it and further processes logs.

# Exception Handling
Required data in the target. At any time when there are some issues between READ and WRITE, an exception should be thrown, Airflow should notify.
Exception handling can be handled in 3 ways:
* based on the application an exception should be thrown
* based on the method an exception should be thrown
* do nothing and let the exception raised => failed job => check and redo it
# Entry Point
# Configuration file (avoid hard-coded values)
*.yaml file so each time when an image is built, config is mounted with the image to create and run a new container.
# Meta file for job control
If the job fails, we do not want to re-process the jobs that already done and want to keep working on the remaining part of the data that already pulled.
