# scripts/etl_process.py

import boto3
import pandas as pd
from io import StringIO
import logging

# -----------------------------
# Configuración
# -----------------------------
RAW_BUCKET = "my-analytics-bucket-v1"
RAW_PREFIX = "raw/"
PROCESSED_BUCKET = "my-analytics-bucket-v1"
PROCESSED_PREFIX = "processed/"
AWS_REGION = "us-east-2"

LOG_FILE = "etl_process.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

s3 = boto3.client("s3", region_name=AWS_REGION)

# -----------------------------
# Funciones
# -----------------------------
def list_raw_files(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return []
    return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]

def download_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    return df

def clean_data(df):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    
    df.dropna(subset=["producto", "categoría", "precio_unitario", "costo_unitario", "unidades_vendidas", "ciudad"], inplace=True)
    
    df["revenue"] = df["precio_unitario"] * df["unidades_vendidas"]
    df["margin"] = df["revenue"] - (df["costo_unitario"] * df["unidades_vendidas"])
    
    return df

def upload_processed_csv(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logging.info(f"Archivo subido a {bucket}/{key}")
    
    # Agregar tags al objeto
    s3.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            'TagSet': [
                {'Key': 'Project', 'Value': 'sales-dashboard'},
                {'Key': 'Stage', 'Value': 'Processed'}
            ]
        }
    )
    logging.info(f"Tags agregados a {bucket}/{key}")

# -----------------------------
# Main
# -----------------------------
def main():
    raw_files = list_raw_files(RAW_BUCKET, RAW_PREFIX)
    if not raw_files:
        logging.info("No se encontraron archivos CSV en la carpeta raw del bucket.")
        print("No hay archivos para procesar.")
        return
    
    for key in raw_files:
        logging.info(f"Procesando archivo: {key}")
        print(f"Procesando {key}...")
        df = download_csv(RAW_BUCKET, key)
        df_clean = clean_data(df)
        
        filename = key.split('/')[-1].replace(".csv", "-clean.csv")
        processed_key = f"{PROCESSED_PREFIX}{filename}"
        upload_processed_csv(df_clean, PROCESSED_BUCKET, processed_key)
        print(f"Archivo procesado, subido y taggeado como {processed_key}")

if __name__ == "__main__":
    main()
