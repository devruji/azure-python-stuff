from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix


def list_blobs_flat(blob_service_client: BlobServiceClient, container_name) -> None:
    container_client: ContainerClient = blob_service_client.get_container_client(
        container=container_name
    )

    blob_list = container_client.list_blobs(name_starts_with="landing/T1B")

    for blob in blob_list:
        print(f"Name: {blob.name}")
