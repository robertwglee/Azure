# This script will copy the latest url report file from RASP platform and upload to Azure storage
# The script will remove the copied files once upload completed
# A cron job will upload url report to Azure storage at 59 minute of each hour
# Logs for this script is located at /var/log/urlreport/azure_upload.log | grep upload


import os
import time
import datetime
from azure.storage.blob import BlockBlobService, PublicAccess
import logging
import sys
import glob
import shutil


# initialize the log settings
logging.basicConfig(filename='/var/log/urlreport/azure_upload.log',level=logging.INFO)

def run_upload():
    try:
        #Copy the latest file from url_report directory
        list_of_files = glob.glob('/home/raspsftp/url_report/*.csv') # * means all if need specific format then *.csv
        latest_file = max(list_of_files, key=os.path.getctime)
        full_path_to_file = '/home/icsu_user/azure_upload/7726report.csv'
        shutil.copy(latest_file,full_path_to_file)
        
        # Check if URL Report exist
        if os.path.exists(full_path_to_file):
         logging.info("URL Report Ready")
        else:
         logging.info("The URL report does not exist")
         sys.exit()
        
        #Connection details for Azure
        storage_account ="projecttangostorageacct"
        token="?sv=2019-02-02&ss=bf&srt=sco&sp=rwdlac&se=2020-04-17T21:07:42Z&st=2020-03-17T13:07:42Z&spr=https&sig=G6cOfje7cLeAO3wTagzanLZB%2Fbh1z9bqu1ZV8knTQCo%3D"

        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(storage_account, sas_token=token)

        # Create a container
        #block_blob_service.create_container(container_name)

        #Specify the container name and rename the file to upload
        container_name ='tangoinput'
        local_file_name = "urls_for_characterization_" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H:%M:%S')+ ".csv"

        # Logging the upload attempts
        #logging.info("Temp file = " + full_path_to_file)
        logging.info("\nUploading to Azure Blob storage as " + local_file_name)

        # Upload the created file, use local_file_name for the blob name
        block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)

        # Log list of files in the Azure container
        logging.info("\nList blobs in the container")
        generator = block_blob_service.list_blobs(container_name)
        for blob in generator:
            logging.info("\t Blob name: " + blob.name)

        # Clean up resources. This includes the container and the temp files
        #block_blob_service.delete_container(container_name)
        
        #Remove the copied file
        os.remove(full_path_to_file)
    except Exception as e:
        logging.exception(e)


# Main method.
if __name__ == '__main__':
    run_upload()
