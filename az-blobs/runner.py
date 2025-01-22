import os, sys
import logging

# Set the logging level for the Azure Identity library
logger = logging.getLogger("azure.identity")
logger.setLevel(logging.DEBUG)

# Direct logging output to stdout. Without adding a handler,
# no logging output is visible.
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

from dotenv import load_dotenv, find_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from list_blobs import list_blobs_flat

load_dotenv(find_dotenv())


credential = DefaultAzureCredential(
    exclude_shared_token_cache_credential=True,
)


if __name__ == "__main__":
    # ?: Create the BlobServiceClient object
    blob_service_client: BlobServiceClient = BlobServiceClient(
        os.environ.get("blob_account_url"), credential=credential
    )

    # ?: Get container client
    # container_client = blob_service_client.get_container_client(
    #     os.environ.get("blob_container_name")
    # )

    # print(os.environ.get("blob_container_name"))
    list_blobs_flat(blob_service_client, os.environ.get("blob_container_name"))
