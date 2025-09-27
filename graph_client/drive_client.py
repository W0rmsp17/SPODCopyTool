from __future__ import annotations
import json
from .graph_common import GRAPH, _enc, _clean

class DriveClient:
    def __init__(self, http):
        self.RH = http

    def ensure_folder_by_path(self, drive, parent_id, name):
        get_url = f"{GRAPH}/drives/{drive}/items/{parent_id}:/{_enc(name)}:?$select=id,name,folder"
        r = self.RH.get(get_url, ok_extra=(404,))
        if r.status_code == 200:
            j = r.json()
            if "folder" in j:
                return j["id"]
            raise RuntimeError(f"Path collision: a file named '{name}' exists at the destination.")
        elif r.status_code != 404:
            r.raise_for_status()

        body = {"name": _clean(name), "folder": {}, "@microsoft.graph.conflictBehavior": "fail"}
        r = self.RH.post(
            f"{GRAPH}/drives/{drive}/items/{parent_id}/children",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body),
        )
        if r.status_code == 409:
            r2 = self.RH.get(get_url, ok_extra=(404,))
            if r2.status_code == 200 and "folder" in r2.json():
                return r2.json()["id"]
        r.raise_for_status()
        return r.json()["id"]

    def try_get_dest_file_fast(self, drive, parent_id, name):
        url = f"{GRAPH}/drives/{drive}/items/{parent_id}:/{_enc(name)}:?$select=id,name,size,file,hashes"
        r = self.RH.get(url, ok_extra=(404,))
        if r.status_code == 200:
            j = r.json()
            if "file" in j:
                h = (j.get("hashes") or {}).get("quickXorHash")
                return j["id"], j.get("size", 0) or 0, h
        elif r.status_code != 404:
            r.raise_for_status()
        return None

    def list_files_map(self, drive, parent):
        url = (f"{GRAPH}/drives/{drive}/root/children?$top=200&$select=id,name,size,folder,file,hashes"
               if parent == "root" else
               f"{GRAPH}/drives/{drive}/items/{parent}/children?$top=200&$select=id,name,size,folder,file,hashes")
        out = {}
        while url:
            j = self.RH.get(url).json()
            for v in j.get("value", []):
                if "folder" in v:
                    continue
                h = (v.get("hashes") or {}).get("quickXorHash")
                out[v["name"]] = (v["id"], v.get("size", 0) or 0, h)
            url = j.get("@odata.nextLink")
        return out

    def list_folders(self, drive_id: str, parent_id: str | None = "root"):
        url = (f"{GRAPH}/drives/{drive_id}/root/children?$top=200&$select=id,name,folder"
               if parent_id in (None, "root") else
               f"{GRAPH}/drives/{drive_id}/items/{parent_id}/children?$top=200&$select=id,name,folder")
        out = []
        while url:
            j = self.RH.get(url).json()
            for v in j.get("value", []):
                if "folder" in v:
                    out.append((v["name"], v["id"]))
            url = j.get("@odata.nextLink")
        return out

    def get_drive_root_id(self, drive_id: str) -> str:
        r = self.RH.get(f"{GRAPH}/drives/{drive_id}/root?$select=id")
        r.raise_for_status()
        return r.json()["id"]
