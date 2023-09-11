## About the ingester

Command line application to ingest the tabular contents of "tall" csv files (access databases with an _.mdb_ or _.accdb_ file extension).

## Built with

* Postgresql 14+ database
* Docker engine 20.10+
* Python 3.8+

## Set up

### pre run
Clone this repo into a local directory. Deposit tall csv files to be ingested into the 'talltables' directory. Include the project metadata _.xlsx_ file within this 'dimas' directory. A _database.ini_ file inside the `/src/utils` directory is required (and not included) with the following format:
```ini
[name_of_the_database_connection]
dbname= ***
host= ***
port= ***
user= ***
password= ***

[alternative_database_connection]
dbname= ***
host= ***
port= ***
user= ***
password= ***

```
### to run the application

1. Build the docker image using the docker-compose engine
```
docker-compose build
```
- This step will assemble all external resources (including the tall tables to be ingested) and introduce them into a docker image.
- Docker will install all the packages required to run the application inside the container.

2. Run the interactive python container
```
docker-compose run --rm ingester
```

- This step will execute the containerized application, provided the docker engine is running on the host machine.

## Usage
