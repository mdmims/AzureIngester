from azure_ingester.helpers.azure_helper import AzureApp
from azure_ingester.adls_client.adls_filesystem import AzureDataLake
from azure_ingester.helpers.aiohttp_helper import aiohttp_handler
from dataclasses import dataclass
from datetime import datetime
from typing import List
import json
import os
import sys


# ISO 3166-1 country codes
COUNTRIES = [
    "US",  # USA
    "GB",  # United Kingdom
    "IT",  # Italy
    "FR",  # France
    "CN",  # China
]


def load_azure_config():
    project_path = os.path.dirname(sys.argv[0])
    with open(f"{project_path}/config/azure_settings.json") as json_file:
        config = json.load(json_file)
    return config


def load_data_to_adls(storage_account_name, account_key, data: List):
    window_folder = datetime.today().strftime("%Y-%m-%d")
    processing_path = f"raw/{window_folder}"
    adl = AzureDataLake(storage_account_name, account_key, "root")
    adl.create_directory(processing_path)
    dir_properties, dir_contents = adl.get_directory_properties("raw", include_paths=True)

    for d in data:
        _data = json.loads(d)
        country = str(_data["country"]).lower()
        window = datetime.today().strftime("%Y%m%d")
        adl.create_file(f"{country}_{window}.json", processing_path, d)


def get_api_data(countries: list = None) -> List[str]:
    """
    :param list countries: list of country codes to fetch data for
    :returns list: list of country specific data
    """
    tasks = []

    def build_payload(target_url):
        return {"method": "GET", "url": target_url, "data": {}}

    # if list of country codes is passed then build the correct url
    # else default to retrieve all countries data
    if countries is not None:
        for c in countries:
            _url = f"http://corona-api.com/countries/{c}"
            tasks.append(build_payload(_url))
    else:
        tasks.append(build_payload("http://corona-api.com/countries"))

    # submit the concurrent requests
    response = aiohttp_handler(tasks)

    # parse and retrieve the relevant data
    country_data = []
    for r in response:
        d = r["data"]
        info = {
            "country": d["name"],
            "country_code": d["code"],
            "population": d["population"],
            "today": d["today"],
            "latest": d["latest_data"],
            "updated_at": d["updated_at"]
        }
        country_data.append(json.dumps(info))

    return country_data


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
    _client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    _adls_account_key = os.environ.get("ACCESS_KEY")
    cfg = AzureConfig(config["tenant_id"], config["application_id"], config["subscription_id"], config["resource_group"], _client_secret,)
    # azure_client = AzureApp(cfg.tenant_id, cfg.client_id, cfg.subscription_id, cfg.resource_group, cfg.client_secret,)
    # _credential = azure_client.oauth_token
    api_data = get_api_data(COUNTRIES)
    load_data_to_adls("datapipelinegen2", _adls_account_key, api_data)
