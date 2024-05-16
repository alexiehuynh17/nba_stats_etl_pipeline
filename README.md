# NBA Player Stats ETL Data Pipeline


## Prerequisites

Before setting up the project, ensure you have the following:

- Docker and Docker Compose installed on your system.
- Apache Airflow Docker image or a custom image with Airflow installed.
- Apache Spark Docker image or a custom image with Spark installed and configured to work with Airflow.
- Docker volumes for Airflow DAGs, logs, and Spark jobs are properly set up.

## Docker Setup

To run this project using Docker, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the directory containing the `docker-compose.yml` file.
3. Build and run the containers using Docker Compose:

```bash
docker-compose up -d --build
```
This command will start the necessary services defined in your docker-compose.yml, such as Airflow webserver, scheduler, Spark master, and worker containers.

## Usage
After the Docker environment is set up, the `crawl_da` DAG will be available in the Airflow web UI [localhost:8080](localhost:8080), where it can be triggered manually or run on its daily schedule.

### Note:
You must add the spark cluster url to the spark connection in the configuration on Airflow UI
