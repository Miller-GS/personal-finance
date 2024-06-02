# personal-finance
ETLs and dashboards to process my financial data.

I'm using Postgres as a database, Python for ingestion scripts, DBT for transformation and Superset for visualization. While Python and
DBT are used directly (ideally by creating a virtual environment and installing the dependencies in requirements.txt), Postgres and Superset can be used with Docker Compose.

## Project Directories

### /financial_data
This is the directory of the DBT project. It will have the DBT project, profile, models, etc.

### /ingestion
Python scripts to load data to the raw layer of Postgres.

### /local_env
This has some files that you'll need to run Postgres and Superset.

### /source_data
Some of the source data has to be inputed manually. This directory has a few samples of mine, and it's where you'll put data of your own.
