from Apidata import retrive_covid,retrive_currency,retrive_Country,merge_resource
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
# API
Covid_url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/npm-covid-data/countries"
Country_url = "https://referential.p.rapidapi.com/v1/country"
Currency_url= "https://api.apilayer.com/exchangerates_data/latest?base=USD"

# staging Area GCS
Covid_stpath = "/home/airflow/data/covid.csv"
Country_stpath = "/home/airflow/data/country.csv"
Currency_stpath =  "/home/airflow/data/currency.csv"


with DAG(
    "Example_ETL_process_Covid_19",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["Personal_project"]
) as dag:

    t1 = PythonOperator(
        task_id="get_data_covid",
        python_callable=retrive_covid,
        op_kwargs={
            "path": Covid_url,
        },
    )

    t2 = PythonOperator(
        task_id="get_data_currency",
        python_callable=retrive_currency,
        op_kwargs={
            "path":Currency_url,
        },
    )

    t3 = PythonOperator(
        task_id="get_data_country",
        python_callable=retrive_Country,
        op_kwargs={
            "path": Country_url ,
        },
    )
    t4 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_resource,
        op_kwargs={
            "covid_fpath": Covid_stpath,
            "currency_fpath":Currency_stpath,
            "country_fpath":Country_stpath
        },
    )
    t5 = GCSToBigQueryOperator(
        task_id='gcs_bq',
        bucket='asia-southeast1-workshop5-ce776d81-bucket',
        source_objects=['data/covid_country.csv'],
        destination_project_dataset_table='workshop5.covidCountry',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )
    
    [t1, t2,t3] >> t4  >> t5