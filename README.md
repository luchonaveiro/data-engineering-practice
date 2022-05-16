# Data Engineering Practice Problems

[![Generic badge](https://img.shields.io/badge/Python-3.6-blue.svg)](https://www.python.org/)
[![Generic badge](https://img.shields.io/badge/pytest-6.2.5-blue.svg)](https://pytest.org/)
[![Generic badge](https://img.shields.io/badge/requests-2.27.1-blue.svg)](https://docs.python-requests.org/)
[![Generic badge](https://img.shields.io/badge/boto3-1.21.2-blue.svg)](https://boto3.readthedocs.io/)
[![Generic badge](https://img.shields.io/badge/pandas-1.1.5-blue.svg)](https://pandas.pydata.org/)
[![Generic badge](https://img.shields.io/badge/pyspark-3.2.0-blue.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Generic badge](https://img.shields.io/badge/PostgreSQL-10.5-blue.svg)](https://www.postgresql.org/)
[![Generic badge](https://img.shields.io/badge/Docker-20.10.6-blue.svg)](https://www.docker.com/)


One of the main obstacles of Data Engineering is the large
and varied technical skills that can be required on a 
day-to-day basis.

This aim of this repository is to help you develop and 
learn those skills. Generally, here are the high level
topics that these practice problems will cover.

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.

## How to work on the problems.
You will need two things to work effectively on most all
of these problems. 
- `Docker`
- `docker-compose`

All the tools and technologies you need will be packaged
  into the `dockerfile` for each exercise.

For each exercise you will need to `cd` into that folder and
run the `docker build` command, that command will be listed in
the `README` for each exercise, follow those instructions.

## Beginner Exercises

### Exercise 1 - Downloading files.
The [first exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-1) tests your ability to download a number of files
from an `HTTP` source and unzip them, storing them locally with `Python`.
`cd Exercises/Exercise-1` and see `README` in that location for instructions.

### Exercise 2 - Web Scraping + Downloading + Pandas
The [second exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-2) 
tests your ability perform web scraping, build uris, download files, and use Pandas to
do some simple cumulative actions.
`cd Exercises/Exercise-2` and see `README` in that location for instructions.

### Exercise 3 - Boto3 AWS + s3 + Python.
The [third exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-3) tests a few skills.
This time we  will be using a popular `aws` package called `boto3` to try to perform a multi-step
actions to download some open source `s3` data files.
`cd Exercises/Exercise-3` and see `README` in that location for instructions.

### Exercise 4 - Convert JSON to CSV + Ragged Directories.
The [fourth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-4) 
focuses more file types `json` and `csv`, and working with them in `Python`.
You will have to traverse a ragged directory structure, finding any `json` files
and converting them to `csv`.

### Exercise 5 - Data Modeling for Postgres + Python.
The [fifth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-5) 
is going to be a little different than the rest. In this problem you will be given a number of
`csv` files. You must create a data model / schema to hold these data sets, including indexes,
then create all the tables inside `Postgres` by connecting to the database with `Python`.


## Intermediate Exercises

### Exercise 6 - Ingestion and Aggregation with PySpark.
The [sixth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-6) 
Is going to step it up a little and move onto more popular tools. In this exercise we are going
to load some files using `PySpark` and then be asked to do some basic aggregation.
Best of luck!

### Exercise 7 - Ingestion and Retrieval with ElasticSearch.
*** IN PROGRESS **
The [seventh exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-7) 
Again, we are going to try a project with another popular Big Data tool, namely 
`ElasticSearch`. Very different from the last project with `PySpark`, but this
exercise will require more attention to detail and fine-tuning. You will
ingest a `.txt` file into a locally running `ElasticSearch` instance and then
retrieve some information from what you just stored.
