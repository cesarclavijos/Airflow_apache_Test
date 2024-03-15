from email.mime.multipart import MIMEMultipart
import pandas as pd
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, date
import requests
from email.mime.text import MIMEText
import smtplib
from db_methods import db_methods


db_methods_execution = db_methods()

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


def enviar():
    try:
        smtp_server = 'smtp.gmail.com'
        smtp_port = 465  # Port for SSL
        sender_email = 'u21100082@unimilitar.edu.co'
        receiver_email = 'cesarclavijo001@gmail.com'
        password = 'rxccrcbnemstzxwe'

        # Create a message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = receiver_email
        message['Subject'] = 'Test Email'

        # Add body to email
        body = 'This is a test email sent using Python.'
        message.attach(MIMEText(body, 'plain'))
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            # Login to SMTP server
            server.login(sender_email, password)
            # Send email
            server.send_message(message)
            print('Email sent successfully!')
    except Exception as exception:
        print(exception)
        print('Failure')


@dag(default_args=default_args, schedule_interval='@daily', start_date=days_ago(2), tags=['Entrega'])
def dag_entrega_final():
    
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
    def save_data_into_db(extracted_data: dict):
        # Guardar informacion
        print("save data in the db")
        json_transformed_data = pd.DataFrame.from_dict(extracted_data,orient='index')
        db_methods_execution.inser_data(json_transformed_data)
        data_saved = True
        return data_saved
    
    @task()
    def envio_de_datos(data_saved : bool):
        # Envio de datos
        if data_saved == True:
            print("Enviando datos")
            enviar()
        else :
            print("error message")


    order_data = extraer()
    order_summary = transformar(order_data)
    data_saved = save_data_into_db(order_summary)
    envio_de_datos(data_saved)
tutorial_etl_dag = dag_entrega_final()