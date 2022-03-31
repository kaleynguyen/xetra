# Applications: a user program built on Spark using its APIs. It consists of a driver and executor on the cluster.
# SparkSession: an object to provide a point of entry. In an interactive Spark shell, the Spark driver instantiates a SparkSession while a Spark application needs to be created a SparkSession
# Job: a parallel computation consisting of multiple tasks that get produced in response to a Spark Action
# Stage: Each job gets divided into smaller sets of tasks called stages that depend on each other.
# Task: a single unit of work or execution that will be sent to a Spark executor. 
