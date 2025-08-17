import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

# Configuración
BUCKET_NAME = "tu-nombre-de-bucket"   # <-- cámbialo por tu bucket real
FILE_PATH = "data/ventas.csv"         # ruta local del dataset
S3_KEY = "raw/ventas.csv"             # ruta en S3 (ej: carpeta raw/)

def upload_to_s3(file_path, bucket, s3_key):
    """Sube un archivo local a un bucket de S3"""
    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(file_path, bucket, s3_key)
        print(f"✅ Archivo {file_path} subido a s3://{bucket}/{s3_key}")
    except FileNotFoundError:
        print("❌ El archivo no existe en la ruta especificada.")
    except NoCredentialsError:
        print("❌ No se encontraron credenciales de AWS. Ejecuta 'aws configure'.")
    except ClientError as e:
        print(f"❌ Error al subir el archivo: {e}")

if __name__ == "__main__":
    if os.path.exists(FILE_PATH):
        upload_to_s3(FILE_PATH, BUCKET_NAME, S3_KEY)
    else:
        print(f"❌ No se encontró el archivo {FILE_PATH}")
