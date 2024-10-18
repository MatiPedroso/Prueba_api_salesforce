import os
import re
import requests
import logging
import pandas as pd
from flask import Flask, make_response
from google.cloud import bigquery
from google.oauth2 import service_account

app = Flask(__name__)

# Configurar el registro de errores
logging.basicConfig(level=logging.INFO)

# Variables globales (usar variables de entorno para credenciales)
url = 'https://login.salesforce.com/services/oauth2/token'
get_line_url = 'https://Matias1234567890.my.salesforce.com/services/data/v49.0/query/?q='
next_page_url = 'https://Matias1234567890.my.salesforce.com'

# Obtener las credenciales de entorno
client_id = os.getenv('SALESFORCE_CLIENT_ID')
client_secret = os.getenv('SALESFORCE_CLIENT_SECRET')
username = os.getenv('SALESFORCE_USERNAME')
password = os.getenv('SALESFORCE_PASSWORD')

# Definir el esquema de la tabla en BigQuery
table_schema = [
    {'name': 'Created_Date', 'type': 'DATE'},
    {'name': 'Numero_Orden', 'type': 'STRING'},
    # Agregar los demás campos del esquema aquí
]

project_id_Matias = 'Matias-produccion'

# Función para obtener el token de autenticación
def get_token():
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'password',
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_data = response.json()
        return f"Bearer {response_data['access_token']}"
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener el token: {e}")
        raise

# Función para obtener los datos de Salesforce
def get_lines(session, token):
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }
    select_query = """
        SELECT 
            CreatedDate, Orden__r.Name, Name, Orden__r.Agencia__r.Name
        FROM Linea__c
    """
    try:
        response = session.get(get_line_url + select_query, headers=headers)
        response.raise_for_status()
        results = response.json()

        # Procesar todos los registros
        all_records = []
        while True:
            all_records.extend(results['records'])
            if not results.get('nextRecordsUrl'):
                break
            response = session.get(next_page_url + results['nextRecordsUrl'], headers=headers)
            results = response.json()
        return all_records
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener los datos: {e}")
        raise

# Función para limpiar y transformar los datos
def clean_and_transform_data(records):
    cleaned_data = []
    for record in records:
        linea = {
            'Created_Date': record['CreatedDate'].split('T')[0],
            'Numero_Orden': record.get('Orden__r', {}).get('Name', 'N/A'),
            'Numero_Linea': record.get('Name', 'N/A'),
            'Agency_Name': record.get('Orden__r', {}).get('Agencia__r', {}).get('Name', 'N/A')
        }
        cleaned_data.append(linea)
    return cleaned_data

# Función para cargar los datos a BigQuery
def load_data_to_bigquery(data):
    credentials = service_account.Credentials.from_service_account_file('ruta_a_tu_archivo_credenciales.json')
    client = bigquery.Client(credentials=credentials, project=project_id_Matias)
    table_id = f"{project_id_Matias}.dataset_name.table_name"

    # Cargar datos al destino en BigQuery
    try:
        job_config = bigquery.LoadJobConfig(schema=table_schema)
        job = client.load_table_from_dataframe(pd.DataFrame(data), table_id, job_config=job_config)
        job.result()
        logging.info(f"Carga completada para {len(data)} registros.")
    except Exception as e:
        logging.error(f"Error al cargar los datos en BigQuery: {e}")
        raise

# Ruta principal para ejecutar el proceso
@app.route('/cargar_datos', methods=['POST'])
def cargar_datos():
    session = requests.Session()
    try:
        token = get_token()
        records = get_lines(session, token)
        cleaned_data = clean_and_transform_data(records)
        load_data_to_bigquery(cleaned_data)
        return make_response({"status": "success", "message": "Datos cargados exitosamente."}, 200)
    except Exception as e:
        logging.error(f"Error general en la ejecución: {e}")
        return make_response({"status": "error", "message": str(e)}, 500)
