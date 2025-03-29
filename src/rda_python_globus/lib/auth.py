import globus_sdk
from globus_sdk.tokenstorage import SimpleJSONFileAdapter

AUTH_RESOURCE_SERVER = "auth.globus.org"
AUTH_SCOPES = ["openid", "profile"]
TRANSFER_RESOURCE_SERVER = "transfer.api.globus.org"
TRANSFFER_SCOPES = "urn:globus:auth:scope:transfer.api.globus.org:all"

CLIENT_CONFIG = '/glade/u/home/rdadata/lib/python/globus_rda_quasar_tokens.json'

RDA_QUASAR_CLIENT_ID = "05c2f58b-c667-4fc4-94fb-546e1cd8f41f"

def token_storage_adapter():
    if not hasattr(token_storage_adapter, "_instance"):
        token_storage_adapter._instance = SimpleJSONFileAdapter(CLIENT_CONFIG, namespace="DEFAULT")
    return token_storage_adapter._instance

def internal_auth_client():
    return globus_sdk.NativeAppAuthClient(RDA_QUASAR_CLIENT_ID, app_name="dsglobus")

def auth_client():
    authorizer = globus_sdk.ClientCredentialsAuthorizer(internal_auth_client(), AUTH_SCOPES)
    return globus_sdk.AuthClient(authorizer=authorizer, app_name="dsglobus")

def transfer_client():
    storage_adapter = token_storage_adapter()
    as_dict = storage_adapter.read_as_dict()

    authdata = as_dict[TRANSFER_RESOURCE_SERVER]
    access_token = authdata["access_token"]
    refresh_token = authdata["refresh_token"]
    access_token_expires = authdata["expires_at_seconds"]
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        refresh_token,
        internal_auth_client(),
        access_token=access_token,
        expires_at=int(access_token_expires),
        on_refresh=storage_adapter.on_refresh,
    )

    return globus_sdk.TransferClient(authorizer=authorizer, app_name="dsglobus")