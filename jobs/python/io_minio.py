from minio import Minio
import os

def load_minio_data(object):
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")  
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
    
    client = Minio("minio:9000", access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)
    tmp_path = f"/opt/airflow/tmps/data/{object}"
    client.fget_object(MINIO_BUCKET_NAME, object, tmp_path)
    
    return tmp_path
