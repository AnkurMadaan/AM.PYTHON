from io import BytesIO
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient,BlobClient,ContainerClient
import azure.functions as func
import os
import tempfile
import xlsxwriter
import traceback



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('<!-- CSV to Excel Convertor function processing a request. -->')

    try:
        # Get the account key and account URL from the environment variables
        account_key = os.environ['AZURE_STORAGE_ACCOUNT_KEY']
        account_url = os.environ['AZURE_STORAGE_ACCOUNT_URL']
        #container_name = os.environ['CONTAINER']
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.info(f"Failed to capture Configuration Settings")
        logging.error(stack_trace)
        return func.HttpResponse(
             f"There is a problem processing you request, Error is logged",
             status_code=500
        )

    try:
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
        container_name = req_body.get('container_name')
        if not container_name:
            return func.HttpResponse(
                "Please pass a container_name in the request body",
                status_code=400
            )
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.info(f"Failed to extract csv_path from the json provided")
        logging.error(stack_trace)
        return func.HttpResponse(
             f"Please provide a proper csv_path json in the request body",
             status_code=400
        )

    try:        
    # Create the blob service client object using the account URL and account key
        blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        # Create the container client object using the blob service client object and the container name
        container_client = blob_service_client.get_container_client(container_name)

        # Download the CSV file from the blob storage using the blob client object and the blob name
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=csv_path)
        download_stream = blob_client.download_blob()
        csv_data = download_stream.readall()
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.info(f"Failed to download csv data ")
        logging.error(stack_trace)
        return func.HttpResponse(
             f"There is a problem processing your request, Error is logged",
             status_code=500
        )

    try:
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
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.info(f"Failed to read, covert or upload excel data ")
        logging.error(stack_trace)
        return func.HttpResponse(
             f"There is a problem processing your request, Error is logged",
             status_code=500
        )
    
    #clean the temp file to avoid space outage issue on Azure Function.
    os.remove(complete_filePath)
    # Return a success message
    return func.HttpResponse(
             f"Successfully converted {csv_path} to {excel_path} in {container_name} container.",
             status_code=200
        )

    # "AzureWebJobsStorage": "",
    # "FUNCTIONS_WORKER_RUNTIME": "python",
    # "AZURE_STORAGE_ACCOUNT_KEY":"",
    # "AZURE_STORAGE_ACCOUNT_URL":"",
    # "CONTAINER":"ankurtest"

    
# azure-functions
# azure.storage.blob
# pandas
# xlsxwriter