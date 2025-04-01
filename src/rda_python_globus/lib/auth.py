import globus_sdk
from globus_sdk.tokenstorage import SimpleJSONFileAdapter
from .config import (
    CLIENT_ID,
    CLIENT_TOKEN_CONFIG,
)

AUTH_RESOURCE_SERVER = "auth.globus.org"
AUTH_SCOPES = ["openid", "profile"]
TRANSFER_RESOURCE_SERVER = "transfer.api.globus.org"
TRANSFFER_SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"

def token_storage_adapter():
    if not hasattr(token_storage_adapter, "_instance"):
        token_storage_adapter._instance = SimpleJSONFileAdapter(CLIENT_TOKEN_CONFIG)
    return token_storage_adapter._instance

def internal_auth_client():
    return globus_sdk.NativeAppAuthClient(CLIENT_ID, app_name="dsglobus")

def auth_client():
    authorizer = globus_sdk.ClientCredentialsAuthorizer(internal_auth_client(), AUTH_SCOPES)
    return globus_sdk.AuthClient(authorizer=authorizer, app_name="dsglobus")

def transfer_client():
    storage_adapter = token_storage_adapter()
    token_data = storage_adapter.get_token_data(TRANSFER_RESOURCE_SERVER)

    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    access_token_expires = token_data["expires_at_seconds"]
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        refresh_token,
        internal_auth_client(),
        access_token=access_token,
        expires_at=int(access_token_expires),
        on_refresh=storage_adapter.on_refresh,
    )

    return globus_sdk.TransferClient(authorizer=authorizer, app_name="dsglobus")