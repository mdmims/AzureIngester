from helpers.azure_helper import AzureApp
import json
import os
import sys


if __name__ == "__main__":
    project_path = os.path.dirname(sys.argv[0])
    with open(f"{project_path}/config/azure_settings.json") as json_file:
        config = json.load(json_file)
    # TODO create cli parser, differnt auth (AD vs token)
    _client_secret = os.environ.get("azure_client_secret")
    azure_client = AzureApp(config["tenant_id"],
                            config["application_id"],
                            config["subscription_id"],
                            config["resource_group"],
                            _client_secret)
    print(azure_client)
