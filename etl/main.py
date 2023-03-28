import functions_framework
import requests
import pandas as pd
import zipfile

import datetime
from google.cloud import bigquery
import pytz
from logging import exception

URL = "https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/geoelectorales/eceg_2020_csv.zip"

def extract_data(url: str) -> pd.DataFrame:
  # Download the file
  response = requests.get(url)
  file_path = "eceg_2020_csv.zip"
  open(file_path, "wb").write(response.content)
  # Unzip file
  with zipfile.ZipFile(file_path, 'r') as zip_ref:
      zip_ref.extractall("")
  # Read the file
  df = pd.read_csv("conjunto_de_datos/INE_DISTRITO_2020.CSV", encoding = "ISO-8859-1")
  # Return dataframe
  return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
  # Modify/Transform column headers
  df.columns = [x.lower() for x in df.columns]
  df = df.astype(str)
  # Return transformed dataframe
  return df


def load_data(df: pd.DataFrame) -> str:
  
  try:
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "dso-dev-377920.inegi.ine_distrito_2020"


    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("entidad", bigquery.enums.SqlTypeNames.STRING)
            
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    return "OK"
  except Exception as e:
    print("########################")
    print(str(e))
    print("########################")
    return "Error"
 
@functions_framework.http
def main(request):
    raw_data = extract_data(URL)
    transformed_data = transform_data(raw_data)
    result = load_data(transformed_data)
    return str(result)
