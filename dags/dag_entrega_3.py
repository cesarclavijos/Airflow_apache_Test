from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from datetime import date


# parametros del DAG
default_args = {
    'owner': 'Cesar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(2), tags=['Entrega'])
def dag_entrega_3():
    
    @task()
    def extraer():
        # Se extrae la informacion de la API y se transforma a diccionario
        url = "https://api.currencybeacon.com/v1/latest?"
        params = {
            "api_key":"ZCkqCc6vqDJNEvczSJqPqz78WiJJeSvQ",
            "base":"USD",
            "symbols":"COP,ADA,AED,AFN,ALL,CAD,BZD,BYN,BWP"}
        response = requests.get(url, params=params)
        data = response.json()
        data_dict = {}
        i=0
        for field_dict in data['response']['rates']:
            data_dict[i] = {'date':data['response']['date'],'base':data['response']['base'], 'To':field_dict ,'rates':data['response']['rates'][field_dict]}
            i=i+1
        order_data_dict = data_dict
        print("extraer data...")
        return order_data_dict
    
    @task(multiple_outputs=True)
    def transformar(order_data_dict: dict):
        # Se tranforma la data se añe columnas
        json_transformed_data = pd.DataFrame.from_dict(order_data_dict,orient='index')
        json_transformed_data['Year']=date.today().year
        json_transformed_data['Month']=date.today().month
        json_transformed_data['Day']=date.today().day
        extracted_data = json_transformed_data.to_dict(orient='index')
        print("trandormar data...")
        return extracted_data
    
    @task()
    def analisis(extracted_data: dict):
        # Se analiza la data
        print("Analizando data...")
        json_transformed_data = pd.DataFrame.from_dict(extracted_data,orient='index')
        for column in json_transformed_data.columns:
            null_values = json_transformed_data[column].isnull().sum()
            print(f"columna '{column}' tiene {null_values} valores nulos")
        print(json_transformed_data.describe())
        print(json_transformed_data.shape)
        print(json_transformed_data.dtypes)
        print(json_transformed_data.info())
        print(json_transformed_data)


    order_data = extraer()
    order_summary = transformar(order_data)
    analisis(order_summary)
tutorial_etl_dag = Entrega_3_cesar()