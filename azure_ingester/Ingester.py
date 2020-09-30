from helpers.azure_helper import AzureApp
from azure_ingester.adls_client.adls_filesystem import AzureDataLake
from dataclasses import dataclass
import json
import os
import sys


def load_azure_config():
    project_path = os.path.dirname(sys.argv[0])
    with open(f"{project_path}/config/azure_settings.json") as json_file:
        config = json.load(json_file)
    return config


def load_data_to_adls(storage_account_name, account_key):
    adl = AzureDataLake(storage_account_name, account_key, "root")
    adl.create_directory("raw")
    dir_properties, dir_contents = adl.get_directory_properties("raw", include_paths=True)


@dataclass(frozen=True)
class AzureConfig:
    tenant_id: str
    client_id: str
    subscription_id: str
    resource_group: str
    client_secret: str


if __name__ == "__main__":
    config = load_azure_config()
    # TODO create cli parser, differnt auth (AD vs token)
    _client_secret = os.environ.get("azure_client_secret")
    _adls_account_key = os.environ.get("access_key")
    cfg = AzureConfig(config["tenant_id"], config["application_id"], config["subscription_id"],
                      config["resource_group"], _client_secret)
    azure_client = AzureApp(cfg.tenant_id, cfg.client_id, cfg.subscription_id, cfg.resource_group, cfg.client_secret)
    _credential = azure_client.oauth_token
    adl_client = load_data_to_adls("datapipelinegen2", _adls_account_key)
