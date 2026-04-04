"""Microsoft Entra ID authentication via service principal."""

import msal


class EntraAuthProvider:
    """Acquires SQL Server access tokens using client credentials flow."""

    _SCOPE = "https://database.windows.net/.default"

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret,
        )

    def get_token(self) -> str:
        """Return a valid access token, using MSAL's built-in cache."""
        result = self._app.acquire_token_for_client(scopes=[self._SCOPE])
        if "access_token" in result:
            return result["access_token"]
        error = result.get("error_description", result.get("error", "Unknown error"))
        raise RuntimeError(f"Token acquisition failed: {error}")
