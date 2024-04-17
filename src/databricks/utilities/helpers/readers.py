import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')
from utilities.spark_session import spark
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from env import container_name, connection_string

def return_latest_file(layer, source):
    # Create a blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the blob container client
    container_client = blob_service_client.get_container_client(container_name)

    # List blobs in the container
    blob_list = container_client.list_blobs()

    latest_blob = None
    latest_timestamp = datetime.min.replace(tzinfo=timezone.utc)

    if layer == "Sourced":
        for blob in blob_list:
            if "Sourced" in blob.name:
                if ".csv" in blob.name:
                    if f"/{source}/" in blob.name:
                        if blob.last_modified > latest_timestamp:
                            latest_blob = blob
                            latest_timestamp = blob.last_modified
            
    if layer == "Cleansed":
        for blob in blob_list:    
            if "Cleansed" in blob.name:
                if ".parquet" in blob.name:
                    if f"/DataFeed={source}/" in blob.name:
                        if blob.last_modified > latest_timestamp:
                            latest_blob = blob
                            latest_timestamp = blob.last_modified
        
    if latest_blob:
        return latest_blob.name


def lake_reader(spark, layer, source):
    file_path = return_latest_file(layer, source)
    adls_path = f"abfss://casestudy@casestudylake.dfs.core.windows.net/{file_path}"
    if layer == "Sourced":
        df = spark.read.csv(adls_path, header="True")
    if layer == "Cleansed":
        df  = spark.read.parquet(adls_path, header="True")
    return df

#sample lake
def read_csv_to_df(spark, file_path):
    '''Reads csv file to a dataframe

    Args:
        file_path (str)
    '''
    df = spark.read.csv(file_path, header="True")
    return df

def read_parquet_to_df(spark, file_path):
    '''Reads parquet file to a dataframe
    Args:
        file_path (string)
    '''
    df = spark.read.parquet(file_path, header="True")
    return df
