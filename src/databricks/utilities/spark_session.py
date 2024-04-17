import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')
from pyspark.sql import SparkSession
from env import storage_account, client_id, directory_id, client_secret

spark = SparkSession.builder\
  .appName("Read from Azure Data Lake")\
  .config(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")\
  .config(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")\
  .config(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)\
  .config(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)\
  .config(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")\
  .config("fs.azure.createRemoteFileSystemDuringInitialization", "true")\
  .getOrCreate()