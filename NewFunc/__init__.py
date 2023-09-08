from io import BytesIO
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient
import azure.functions as func
import os
import tempfile
import xlsxwriter



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get the account key and account URL from the environment variables
    account_key = os.environ['AZURE_STORAGE_ACCOUNT_KEY']
    account_url = os.environ['AZURE_STORAGE_ACCOUNT_URL']

    # Get the JSON object from the request body
    req_body = req.get_json()

    # Get the path to the CSV file from the JSON object
    csv_path = req_body.get('csv_path')

    # Validate the input
    if not csv_path:
        return func.HttpResponse(
             "Please pass a csv_path in the request body",
             status_code=400
        )

    # Create the blob service client object using the account URL and account key
    blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)

    # Create the container client object using the blob service client object and the container name
    container_name = os.environ['CONTAINER']
    container_client = blob_service_client.get_container_client(container_name)

    # Download the CSV file from the blob storage using the blob client object and the blob name
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_path)
    download_stream = blob_client.download_blob()
    csv_data = download_stream.readall()

    # Read the CSV file using pandas
    df = pd.read_csv(BytesIO(csv_data))
    filename = blob_client.blob_name.replace('.csv','.xlsx');
    temppath = tempfile.gettempdir();
    complete_filePath = os.path.join(temppath,filename)


    # Convert the CSV file to an Excel file using pandas
    writer = pd.ExcelWriter(complete_filePath, engine='xlsxwriter')
    excel_data = df.to_excel(writer,index=None, header=True)
    writer.close()

    # Upload the Excel file to the same blob storage using the blob client object and the blob name
    excel_path = csv_path.replace('.csv', '.xlsx')

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=excel_path)
    with open(complete_filePath,"rb") as xlsx:
        xlsxdata = blob_client.upload_blob(overwrite=True,data=xlsx)
    
    # Return a success message
    return func.HttpResponse(
             f"Successfully converted {csv_path} to {excel_path} in {container_name} container.",
             status_code=200
        )

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
