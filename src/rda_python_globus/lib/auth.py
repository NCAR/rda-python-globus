import globus_sdk
from globus_sdk.tokenstorage import JSONTokenStorage
from .config import (
    QUASAR_CLIENT_ID,
    TACC_CLIENT_ID,
    CLIENT_TOKEN_CONFIG,
    TACC_TOKEN_CONFIG,
)

AUTH_RESOURCE_SERVER = "auth.globus.org"
AUTH_SCOPES = ["openid", "profile"]
TRANSFER_RESOURCE_SERVER = "transfer.api.globus.org"
TRANSFFER_SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"

def token_storage_adapter(namespace="quasar"):
    """ Return a JSONTokenStorage instance for the specified namespace. If an instance already exists, return the existing instance. """
    if namespace == "tacc":
        json_config = TACC_TOKEN_CONFIG
    else:
        json_config = CLIENT_TOKEN_CONFIG
    
    if not hasattr(token_storage_adapter, "_instance"):
        token_storage_adapter._instance = JSONTokenStorage(json_config, namespace=namespace)
    return token_storage_adapter._instance

def internal_auth_client(client_id=QUASAR_CLIENT_ID):
    """ Return a NativeAppAuthClient instance for the specified client ID. """
    return globus_sdk.NativeAppAuthClient(client_id, app_name="dsglobus")

def auth_client():
    authorizer = globus_sdk.ClientCredentialsAuthorizer(internal_auth_client(), AUTH_SCOPES)
    return globus_sdk.AuthClient(authorizer=authorizer, app_name="dsglobus")

def transfer_client(namespace="quasar"):
    """ Return a TransferClient instance for the specified namespace. """

    if namespace == 'tacc':
        client_id = TACC_CLIENT_ID
    else:
        client_id = QUASAR_CLIENT_ID

    auth_client = internal_auth_client(client_id=client_id)

    storage_adapter = token_storage_adapter(namespace)
    token_data = storage_adapter.get_token_data(TRANSFER_RESOURCE_SERVER)
    access_token = token_data.access_token
    refresh_token = token_data.refresh_token
    access_token_expires = token_data.expires_at_seconds

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        refresh_token,
        auth_client,
        access_token=access_token,
        expires_at=int(access_token_expires),
        on_refresh=storage_adapter.store_token_response(auth_client.oauth2_refresh_token(refresh_token)),
    )
    return globus_sdk.TransferClient(authorizer=authorizer, app_name="dsglobus")