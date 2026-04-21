"""Microsoft Fabric REST API client for resolving SQL connection strings."""

import msal
import requests


_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_BASE = "https://api.fabric.microsoft.com/v1"


class FabricClient:
    """Resolves Fabric warehouse / lakehouse SQL connection strings."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._app = msal.ConfidentialClientApplication(
            client_id, authority=authority, client_credential=client_secret,
        )

    def _get_token(self) -> str:
        result = self._app.acquire_token_for_client(scopes=[_FABRIC_SCOPE])
        if "access_token" in result:
            return result["access_token"]
        error = result.get("error_description", result.get("error", "Unknown error"))
        raise RuntimeError(f"Fabric token acquisition failed: {error}")

    def _headers(self):
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ── workspace lookup ──────────────────────────────────

    def _find_workspace_id(self, workspace_name: str) -> str:
        """Return the workspace ID for *workspace_name* (case-insensitive)."""
        url = f"{_BASE}/workspaces"
        target = workspace_name.lower()
        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            body = resp.json()
            for ws in body.get("value", []):
                if ws["displayName"].lower() == target:
                    return ws["id"]
            url = body.get("continuationUri")
        raise LookupError(f"Workspace '{workspace_name}' not found.")

    # ── warehouse ─────────────────────────────────────────

    def _try_warehouse(self, workspace_id: str, item_name: str):
        """Return (connection_string, database_name, 'Warehouse') or None."""
        url = f"{_BASE}/workspaces/{workspace_id}/warehouses"
        target = item_name.lower()
        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            body = resp.json()
            for wh in body.get("value", []):
                if wh["displayName"].lower() == target:
                    props = wh.get("properties", {})
                    conn = props.get("connectionString")
                    if not conn:
                        # Fetch individual item for full properties
                        detail = requests.get(
                            f"{_BASE}/workspaces/{workspace_id}/warehouses/{wh['id']}",
                            headers=self._headers(), timeout=30,
                        )
                        detail.raise_for_status()
                        props = detail.json().get("properties", {})
                        conn = props.get("connectionString")
                    if not conn:
                        raise LookupError(
                            f"Warehouse '{item_name}' found but has no connection string."
                        )
                    return conn, wh["displayName"], "Warehouse"
            url = body.get("continuationUri")
        return None

    # ── lakehouse / SQL analytics endpoint ────────────────

    def _try_lakehouse(self, workspace_id: str, item_name: str):
        """Return (connection_string, database_name, 'Lakehouse') or None."""
        url = f"{_BASE}/workspaces/{workspace_id}/lakehouses"
        target = item_name.lower()
        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            body = resp.json()
            for lh in body.get("value", []):
                if lh["displayName"].lower() == target:
                    props = lh.get("properties", {})
                    sql_props = props.get("sqlEndpointProperties", {})
                    conn = sql_props.get("connectionString")
                    if not conn:
                        # Fetch individual item for full properties
                        detail = requests.get(
                            f"{_BASE}/workspaces/{workspace_id}/lakehouses/{lh['id']}",
                            headers=self._headers(), timeout=30,
                        )
                        detail.raise_for_status()
                        props = detail.json().get("properties", {})
                        sql_props = props.get("sqlEndpointProperties", {})
                        conn = sql_props.get("connectionString")
                    if not conn:
                        status = sql_props.get("provisioningStatus", "unknown")
                        raise LookupError(
                            f"Lakehouse '{item_name}' found but SQL endpoint "
                            f"has no connection string (status: {status})."
                        )
                    return conn, lh["displayName"], "Lakehouse"
            url = body.get("continuationUri")
        return None

    # ── public API ────────────────────────────────────────

    def resolve(self, workspace_name: str, item_name: str):
        """Resolve a Fabric item to its SQL connection string.

        Tries warehouse first, then lakehouse/SQL analytics endpoint.

        Returns:
            (server, database, item_type)  where *server* is the SQL endpoint
            hostname, *database* is the item display name, and *item_type* is
            ``'Warehouse'`` or ``'Lakehouse'``.
        """
        ws_id = self._find_workspace_id(workspace_name)

        result = self._try_warehouse(ws_id, item_name)
        if result:
            return result

        result = self._try_lakehouse(ws_id, item_name)
        if result:
            return result

        raise LookupError(
            f"'{item_name}' not found as a Warehouse or Lakehouse "
            f"in workspace '{workspace_name}'."
        )
