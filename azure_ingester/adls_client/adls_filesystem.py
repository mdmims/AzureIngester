from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from typing import Dict, Tuple, Union


class AzureDataLake:
    """
    Initialize DataLake client to a specific Container
    """
    def __init__(self, storage_account_name: str, storage_account_key: str, container_name: str):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.container_name = container_name
        self.adls_client = self.initialize_storage_account()
        self.file_system_client = self.get_filesystem_client()

    def initialize_storage_account(self):
        url = f"https://{self.storage_account_name}.dfs.core.windows.net"
        try:
            service_client = DataLakeServiceClient(account_url=url, credential=self.storage_account_key)
        except Exception as e:
            print(e)

        return service_client

    def get_filesystem_client(self):
        """
        Initialize the file system client for a container
        """
        try:
            file_system = self.adls_client.get_file_system_client(file_system=self.container_name)
        except ResourceExistsError:
            file_system = self.adls_client.create_file_system(file_system=self.container_name)
        return file_system

    def create_directory(self, directory_name: str):
        """
        Create a directory within specified container
        """
        return self.file_system_client.create_directory(directory_name)

    def get_director_client(self, path: str = None):
        if path is None:
            fs = self.container_name
        else:
            fs = path

        return self.file_system_client.get_directory_client(fs)

    def get_directory_properties(self, directory_name: str, include_paths: bool = False
                                 ) -> Tuple[Dict, Union[str, None]]:
        """
        :param str directory_name: Name of directory/folder
        :param bool include_paths: Return list of files/directories at specified directory
        """
        directory = self.file_system_client.get_directory_client(directory=directory_name)
        properties = directory.get_directory_properties()
        _properties = {k: v for (k, v) in properties.__dict__.items()}

        if include_paths:
            paths = self.file_system_client.get_paths(path=directory_name)
            _paths = [p.name for p in paths]
        else:
            _paths = None
        return _properties, _paths

    def create_file(self, filename: str, filepath: str, content) -> None:
        dc = self.get_director_client(path=filepath)
        _file = dc.create_file(filename)
        _file.append_data(content, 0, len(content))
        _file.flush_data(len(content))
