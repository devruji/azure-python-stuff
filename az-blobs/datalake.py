import os
import pandas as pd

from io import BytesIO
from dotenv import load_dotenv, find_dotenv

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileSystemClient,
    DataLakeFileClient,
)

# ?: Load the environment variables from the .env file
load_dotenv(find_dotenv())


def get_service_client(storage_account_name: str) -> DataLakeServiceClient:
    """
    The function `get_service_client` returns a DataLakeServiceClient object for a
    given storage account name.

    :param storage_account_name: The `storage_account_name` parameter is the name of
    the Azure Storage account that you want to connect to in order to access the
    Data Lake service. This name is used to construct the account URL for the Data
    Lake service client
    :return: A DataLakeServiceClient object is being returned.
    """
    credential = DefaultAzureCredential(
        exclude_shared_token_cache_credential=True,
    )
    account_url = f"https://{storage_account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=credential)


def read_excel_from_adls(
    storage_account_name: str, filesystem_name: str, file_path: str
) -> pd.DataFrame:
    """
    This Python function reads an Excel file from Azure Data Lake Storage and loads
    its content into a Pandas DataFrame.

    :param storage_account_name: The `storage_account_name` parameter refers to the
    name of the Azure Data Lake Storage account where the Excel file is located.
    This is used to establish a connection to the Data Lake Storage account
    :type storage_account_name: str
    :param filesystem_name: The `filesystem_name` parameter in the
    `read_excel_from_adls` function refers to the name of the Azure Data Lake
    Storage Gen2 file system where the Excel file is located. This parameter is used
    to specify the specific file system within the storage account from which the
    Excel file will be read
    :type filesystem_name: str
    :param file_path: The `file_path` parameter in the `read_excel_from_adls`
    function refers to the path of the Excel file stored in the Azure Data Lake
    Storage (ADLS) that you want to read and load into a Pandas DataFrame. This path
    should include the directory structure within the specified file system
    :type file_path: str
    :return: The function `read_excel_from_adls` returns a Pandas DataFrame
    containing the data from an Excel file stored in Azure Data Lake Storage.
    """
    service_client: DataLakeServiceClient = get_service_client(storage_account_name)

    file_system_client: FileSystemClient = service_client.get_file_system_client(
        file_system=filesystem_name
    )
    file_client: DataLakeFileClient = file_system_client.get_file_client(file_path)

    # ?: Download the file content
    download_stream = file_client.download_file()
    file_content: bytes = download_stream.readall()

    # ?: Load the content into a Pandas DataFrame
    excel_data: BytesIO = BytesIO(file_content)
    df: pd.DataFrame = pd.read_excel(excel_data, engine="openpyxl")

    return df


if __name__ == "__main__":
    try:
        df: pd.DataFrame = read_excel_from_adls(
            storage_account_name=os.environ.get("STORAGE_ACCOUNT_NAME"),
            filesystem_name=os.environ.get("CONTAINER_NAME"),
            file_path=os.path.join(
                os.environ.get("FILE_PATH"), os.environ.get("FILE_NAME")
            ),
        )
        print(df.head())

        df.to_parquet("to-be-removed.parquet", index=False, engine="fastparquet")

    except Exception as e:
        print(f"Error: {e}")
