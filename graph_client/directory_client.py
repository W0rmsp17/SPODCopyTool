from __future__ import annotations

from .graph_common import GRAPH, _enc, _clean

class DirectoryClient:
    def __init__(self, http):
        self.RH = http
#Resolve f
    def resolve_site_id_from_url(self, site_url: str):
        host, path = _parse_site_url(site_url)
        r = self.RH.get(f"{GRAPH}/sites/{host}:{path}")
        r.raise_for_status()
        j = r.json()
        return j["id"], j.get("displayName")

    def list_site_libraries(self, site_id: str):
        url = f"{GRAPH}/sites/{site_id}/drives?$select=id,name,driveType"
        libs = []
        while url:
            j = self.RH.get(url).json()
            for d in j.get("value", []):
                if d.get("driveType") == "documentLibrary":
                    libs.append({"id": d["id"], "name": d["name"], "driveType": d["driveType"]})
            url = j.get("@odata.nextLink")
        return libs

    def search_sites(self, query="*"):
        url = f"{GRAPH}/sites?search={query}&$select=id,displayName,webUrl"
        sites = []
        while url:
            j = self.RH.get(url).json()
            for s in j.get("value", []):
                sites.append({
                    "id": s["id"],
                    "name": s.get("displayName") or s.get("webUrl") or s["id"],
                    "webUrl": s.get("webUrl")
                })
            url = j.get("@odata.nextLink")
        return sites

    def search_users(self, query: str, top: int = 25):
        q = (query or "").strip().replace("'", "''")
        if not q:
            return []
        flt = f"startswith(displayName,'{q}') or startswith(userPrincipalName,'{q}')"
        params = {
            "$select": "id,displayName,userPrincipalName",
            "$filter": flt,
            "$orderby": "displayName",
            "$top": str(top),
            "$count": "true",
        }
        j = self.RH.get(
            f"{GRAPH}/users",
            params=params,
            headers={"ConsistencyLevel": "eventual"},
        ).json()
        return [{
            "id": u["id"],
            "upn": u.get("userPrincipalName"),
            "name": u.get("displayName") or u.get("userPrincipalName"),
        } for u in j.get("value", [])]

    def resolve_user_drive(self, upn_or_id: str):
        r = self.RH.get(f"{GRAPH}/users/{upn_or_id}/drive", ok_extra=(404,))
        if r.status_code == 200:
            j = r.json()
            return j["id"], j.get("name") or "OneDrive"
        if r.status_code == 404:
            return None
        r.raise_for_status()
