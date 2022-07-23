import numpy as np
import requests
import pandas as pd
import json
import csv
import os
from google.cloud import storage
## API
Covid_api = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/npm-covid-data/countries"
Country_api = "https://referential.p.rapidapi.com/v1/country"
Currency_api = "https://api.apilayer.com/exchangerates_data/latest?base=USD"

class Config:
    CURRENCY_KEY = os.getenv("CURRENCY_KEY")
    COVID_KEY =  os.getenv("COVID_KEY")
    COUNTRY_KEY = os.getenv("COUNTRY_KEY")
    GCP_JSONKEY = os.getenv("GCP_JSONKEY")
	BUCKET_NAME = os.getenv("BUCKET_NAME")


## path
output_path = "/home/airflow/data/"

## staging Area
bucket_name = Config.BUCKET_NAME ## your bucket name 

## gcs authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= Config.GCP_JSONKEY# .json GCP Key

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

def missing_replace(missing_dict, data, col):
	for i in missing_dict:
		data[data[col] == i] = data[data[col] == i].replace(
			(missing_dict.get(i)))
	return data

def retrive_covid(path):
	##Call api
	headers = {
		"X-RapidAPI-Key": Config.COVID_KEY, # your api key
		"X-RapidAPI-Host": "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com"
	}
	Covid_data = requests.request("GET", path, headers=headers).text
	Covid_data.replace(Covid_data[0], " ")
	Covid_dict = json.loads(Covid_data)


	# load to dataframe
	df_covid = pd.DataFrame(Covid_dict)

	## Cleansing
	df_covid = df_covid.convert_dtypes()

	##Save to csv
	df_covid.to_csv(output_path + "covid.csv", index=False)

	## upload to gcs
	upload_blob(bucket_name, output_path + "covid.csv" ,"data/covid.csv")


def retrive_currency(path):

	##call api
	headers= {
	  "apikey": Config.CURRENCY_KEY# your api key
	}
	currency_data = requests.request("GET", path, headers=headers).text
	currency_data = currency_data.replace("\n", "")
	currency_dict = json.loads(currency_data)


	## remove key
	remove = ['success', 'timestamp']
	currency_dict = {key: currency_dict[key] for key in currency_dict if key not in remove}
	## load to dataframe
	df_currency = pd.DataFrame(currency_dict)
	df_currency = df_currency.rename_axis("currency").reset_index()


	##Cleansing
		##drop col
	df_currency = df_currency.drop(columns="date").rename(columns={"currency": "currency_code"})

		## type
	df_currency = df_currency.convert_dtypes()
	df_currency["rates"] = df_currency["rates"].astype("float64")

		##Web scaping write some function to scaping currency
	SSP_currency = {"currency_code": "SSP", "base": "USD", "rates": 130.26}
	df_currency = df_currency.append(SSP_currency, ignore_index=True)

	##Save to csv
	df_currency.to_csv(output_path + "currency.csv", index=False)

	## upload to gcs
	upload_blob(bucket_name, output_path + "currency.csv" ,"data/currency.csv")

def retrive_country(path):
	## Function for fix missing_value

	## Call API
	querystring = {"fields":"currency,currency_num_code,currency_code,continent_code,currency,iso_a3,dial_code","limit":"250"}

	headers = {
		"X-RapidAPI-Key": Config.COUNTRY_KEY , # your api key
		"X-RapidAPI-Host": "referential.p.rapidapi.com"
	}

	country = requests.request("GET", path, headers=headers, params=querystring)
	country_data = country.text
	country_dict = json.loads(country_data)

	# load to dataframe
	df_country = pd.DataFrame(country_dict)


	## Cleansing
	df_country = df_country.convert_dtypes() ## change type
	df_country["key"] = df_country["key"].apply(lambda x: x.lower()) ## lower case
		## drop column, change column name
	df_country["Country"] = df_country["value"]
	df_country["TwoLetterSymbol"] = df_country["key"]
	df_country = df_country.drop(columns=["iso_a3", "currency_num_code", "value", "key"])

		## missing value
	missingCountry_dict = {"Taiwan":{'currency_code': {None: "TWD"}, "currency": {None: "New Taiwan dollar"}},
						   "Kosovo":{'currency_code': {None: "EUR"}, "currency": {None: "Euro"}}
						   }
	df_country = missing_replace(missingCountry_dict, df_country, "Country")

		## Change currency
	missingCurrency_dict = { "Bhutan" :  {'currency_code': {"INR,BTN" : "BTN"} ,  "currency" : {"Indian Rupee,Ngultrum":"Ngultrum"}},
					 "Cuba" : {'currency_code': {"CUP,CUC": "CUP"}, "currency": {"Cuban Peso,Peso Convertible": "Cuban Peso"}},
					 "El Salvador" :  {'currency_code': {"SVC,USD": "SVC"}, "currency": {"El Salvador Colon,US Dollar": "El Salvador Colon"}},
					 "Haiti " :  {'currency_code': {"HTG,USD": "HTG"}, "currency": {"Gourde,US Dollar": "Gourde"}},
					 "Lesotho" :  {'currency_code': {"LSL,ZAR": "LSL"}, "currency": {"Loti,Rand": "Loti"}},
					 "Namibia" : {'currency_code': {"NAD,ZAR": "NAD"}, "currency": {"Namibia Dollar,Rand": "Namibia Dollar"}},
					 "Panama" : {'currency_code': {"PAB,USD" : "PAB"} ,  "currency" : {"Balboa,US Dollar":"Balboa"}}
	}
	df_country  = missing_replace(missingCurrency_dict,df_country,"Country")

	# save to csv
	df_country.to_csv(output_path + "country.csv", index=False)

	## upload to gcs
	upload_blob(bucket_name, output_path + "country.csv" ,"data/country.csv")
	

def merge_resource(covid_fpath,currency_fpath,country_fpath):
	# load from google cloud storage
	download_blob(bucket_name,"data/covid.csv",covid_fpath)
	download_blob(bucket_name,"data/currency.csv",currency_fpath)
	download_blob(bucket_name,"data/country.csv",country_fpath)
	## read file
	df_covid  = pd.read_csv(covid_fpath)
	df_currency = pd.read_csv(currency_fpath)
	df_country= pd.read_csv(country_fpath)

	# JOIN CURRENCY AND COUNTRY
	final_country = df_country.merge(df_currency, how="left", left_on="currency_code", right_on="currency_code")

	# Covid join Country
	Covid_Country = df_covid.merge(final_country, how="inner", left_on="TwoLetterSymbol", right_on="TwoLetterSymbol")

	# cleansing data
	Covid_Country = Covid_Country.drop(columns="Country_y")
	new_column = {"Country_x": "Country", "TwoLetterSymbol": "Country_code2", "ThreeLetterSymbol": "Country_code3"}
	Covid_Country = Covid_Country.rename(columns=new_column)

	# save to csv
	Covid_Country.to_csv(output_path + "covid_country.csv") # save to csv

	## upload to gcs
	upload_blob(bucket_name, output_path + "covid_country.csv" ,"data/covid_country.csv")



