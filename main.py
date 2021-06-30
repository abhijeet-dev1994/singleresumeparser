
import os
import urllib.parse
from pymongo import MongoClient

mongo_uri = "mongodb+srv://Abhijeet:" + urllib.parse.quote(
    "somit@30") + "@cluster0.nfrjt.mongodb.net/parsing?retryWrites=true&w=majority"
client = MongoClient(mongo_uri)
db = client["parsing"]
schema = db["resumes"]
from resume_parser import resumeparse
from multiprocessing.pool import ThreadPool
from azure.storage.blob import BlobServiceClient, BlobClient

# IMPORTANT: Replace connection string with your storage account connection string
# Usually starts with DefaultEndpointsProtocol=https;...
MY_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=shareimage;AccountKey=inYVAD5E4BPCowKkvHUzpKOwdulpPhJ8ysBkfkF6Y7yGRx5eyUvYMw/QpxbflQ+p2PB2qe7FDcoi6kPrvhb7YA==;EndpointSuffix=core.windows.net"

# Replace with blob container name
MY_BLOB_CONTAINER = "icprofileimage"
bModifiedOn = ""
# Replace with the local folder where you want downloaded files to be stored
LOCAL_BLOB_PATH = "/home/abhijeet/live_all_resumes/"


class AzureBlobFileDownloader:
    def __init__(self):
        print("Intializing AzureBlobFileDownloader")

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(MY_BLOB_CONTAINER)

    def download_all_blobs_in_container(self):
        # get a list of blobs
        my_blobs = self.my_container.list_blobs()
        result = self.run(my_blobs)
        print(result)

    def run(self, blobs):
        # Download 10 files at a time!
        with ThreadPool(processes=int(10)) as pool:
            return pool.map(self.save_blob_locally, blobs)

    def save_blob_locally(self, blob):
        try:
            file_name = blob.name
            ##bModifiedOn = blob.last_modified
            bytes = self.my_container.get_blob_client(blob).download_blob().readall()
            # Get full path to the file
            download_file_path = os.path.join(LOCAL_BLOB_PATH, file_name)
            # for nested blobs, create local path as well!
            os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
            with open(download_file_path, "wb") as file:
                file.write(bytes)
                data = resumeparse.read_file(LOCAL_BLOB_PATH + file_name)
                if ((data['email'] != None) and (data['phone'] != '')):
                    result = schema.insert_one(data);
                    print("resumedata", data)
                else:
                    print("cannot parse")
            return file_name

        except:
            print("Container not found.")

# Initialize class and upload files
##azure_blob_file_downloader = AzureBlobFileDownloader()
##azure_blob_file_downloader.download_all_blobs_in_container()
