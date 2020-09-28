import requests
import json
import time


MAX_API_RETRY = 5


def fatal_code(response) -> bool:
    """
    Return True/False if error response status code is within fatal http range
    """
    return 400 <= response.status_code < 500


class AzureApp:
    """
    Use Azure AD credentials to auth to Azure services
    """
    def __init__(self, tenant_id, application_id, subscription_id, resource_group, client_secret):
        self.tenant_id = tenant_id
        self.client_id = application_id
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.client_secret = client_secret
        self._oauth_token = None
        self.grace_period = 300  # attempt to refresh azure oauth token after 300 seconds

    @property
    def oauth_token(self):
        if not self._oauth_token or float(self._oauth_token.get("expires_on")) < time.time() + self.grace_period:
            azure_auth_url = f"https://login.microsoft.com/{self.tenant_id}/oauth2/token"
            azure_grant_type = "client_credentials"
            azure_resource = "https://management.azure.com/"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {
                "grant_type": azure_grant_type,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "resource": azure_resource,
            }
            try:
                response = requests.post(azure_auth_url, headers=headers, data=data).json()
            except Exception as e:
                raise e

            if "error" in response:
                raise ValueError(json.dumps(response))
            else:
                self._oauth_token = response

        return self._oauth_token.get("access_token")
