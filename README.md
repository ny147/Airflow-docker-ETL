# Airflow-docker-ETL
Example project ETL with apache airflow on docker
## Workflow
![workflow](https://user-images.githubusercontent.com/60291649/180922949-26c32eb5-22f6-4008-9329-0dd28917f891.PNG)
## Run project
- clone this repository
- Run ```docker-compose up airflow-init```
- Run ```docker build -t apache/airflow:2.3.2 .``` for install package
- Run ``` docker-compose up ``` 
## Airflow with docker
- Follow  [Running Airflow in Docker] (https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html). including create dags, logs, plugins, and data folders.
