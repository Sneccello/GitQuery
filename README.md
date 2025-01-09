# GitBridge

## About
This tool allows users to compare statistics about repositories and query
data about contributors across repositories. 
You can ingest git repositories and make 
queries like "Who created the most files in all repositories"
or "Which repository has the most deleted files".
Data is uploaded to HDFS and queries are made with Spark, all containerized with Docker.

## Quickstart

```
docker compose up [--scale spark-worker={#desiredWorkers}]
```  
This will launch 
- HDFS containers
  - datanode
  - namenode with WebUI on [http://localhost:9870](http://localhost:9870)
- Spark containers
  - Spark Master with WebUI on [http://localhost:9870](http://localhost:9870)
  - Spark workers (scaled with the command above)
- The streamlit app to interact on [http://localhost:8501](http://localhost:8501)
- A jupyter notebook environment in the docker network for free interaction on
[http://localhost:8888](http://localhost:8888)

## How it works

### Ingest
To get the data, the User has to give a git repository URL and optionally
partitioning columns to control how the data is stored on HDFS - 
potentially supporting their SQL queries mentioned later.
Once cloned, a gitlog file is generated and uploaded to HDFS.
After this Spark (with the app running the driver code) will read
the file and parse it to a dataframe containing information
about file changes, author, date, repository etc.
The RDD is then written back to HDFS in parquet next to the other repositories.
![ingest diagram](ingest-diag.drawio.svg)

### Querying

Querying involves reading the ingested git metadata with Spark
and running queries on the RDD. 
The results are then visualized on the app with streamlit.
![query diagram](query-diag.drawio.svg)
A few diagrams are auto-generated, but in the streamlit MPA, a SQL interface is
also provided to interact with spark directly. You can run own your SQL commands
on the Spark RDD and see the result dataframe directly on the app. Additionally,
you can take a look at the spark query plan, with some operations highlighted,
such as utilized partition filtering when reading the dataset for the query.

