from __future__ import annotations

from .graph_common import GRAPH, _enc

class TransferManager:
    def __init__(
        self,
        http,
        drive_client,
        *,
        chunk=8*1024*1024,
        min_chunk=1*1024*1024,
        max_single=4*1024*1024,
        get_cursor=None,
        set_cursor=None,
        clear_cursor=None,
        should_cancel=None,
    ):
        self.RH = http
        self.drive = drive_client
        self.CHUNK = chunk
        self.MIN_CHUNK = min_chunk
        self.MAX_SINGLE = max_single

        # resume/cancel hooks (controller supplies these)
        self.get_cursor = get_cursor or (lambda folder_id: None)
        self.set_cursor = set_cursor or (lambda folder_id, name: None)
        self.clear_cursor = clear_cursor or (lambda folder_id: None)
        self.should_cancel = should_cancel or (lambda: False)

    #Downloads/Uploads
    def _download_entire(self, drive, item_id):
        r = self.RH.get(f"{GRAPH}/drives/{drive}/items/{item_id}/content")
        r.raise_for_status()
        return r.content

    def _download_range(self, drive, item_id, start, length):
        attempts = 0
        while attempts < 8:
            try_len = max(self.MIN_CHUNK, min(length, self.CHUNK >> attempts))
            end = start + try_len - 1
            r = self.RH.get(
                f"{GRAPH}/drives/{drive}/items/{item_id}/content",
                headers={"Range": f"bytes={start}-{end}"}
            )
            if r.status_code in (200, 206):
                return r.content
            attempts += 1
        if start == 0 and length <= self.MAX_SINGLE:
            r = self.RH.get(f"{GRAPH}/drives/{drive}/items/{item_id}/content")
            if r.status_code == 200:
                return r.content
        raise RuntimeError(f"range GET failed: {item_id} bytes {start}-{start+length-1}")

    def _upload_small_replace(self, dest_drive, dest_parent_id, name, content_bytes):
        url = f"{GRAPH}/drives/{dest_drive}/items/{dest_parent_id}:/{_enc(name)}:/content"
        r = self.RH.put(url, headers={"Content-Type": "application/octet-stream"}, data=content_bytes)
        if r.status_code not in (200, 201, 202):
            r.raise_for_status()
        return r

    def _create_upload_session(self, dest_drive, dest_parent_id, name):
        r = self.RH.post(
            f"{GRAPH}/drives/{dest_drive}/items/{dest_parent_id}:/{_enc(name)}:/createUploadSession",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"@microsoft.graph.conflictBehavior": "replace"})
        )
        r.raise_for_status()
        return r.json()["uploadUrl"]

    def _get_session_status(self, upload_url):
        r = self.RH.get(upload_url, ok_extra=(404, 410))
        if r.status_code in (404, 410):
            return None
        return r.json()

    def _parse_next_start(self, status_json):
        rngs = (status_json or {}).get("nextExpectedRanges") or []
        if not rngs:
            return None
        first = rngs[0]
        start = first.split("-", 1)[0]
        try:
            return int(start)
        except Exception:
            return None

    def _upload_session_put(self, url, chunk, start, total, max_tries=12):
        hdr = {"Content-Length": str(len(chunk)), "Content-Range": f"bytes {start}-{start+len(chunk)-1}/{total}"}
        return self.RH.put(url, headers=hdr, data=chunk, max_tries=max_tries)

    def upload_stream_replace(self, dest_drive, dest_parent_id, name, src_drive, src_item_id, total_size):
        if total_size <= self.MAX_SINGLE:
            blob = self._download_entire(src_drive, src_item_id)
            return self._upload_small_replace(dest_drive, dest_parent_id, name, blob)

        upload_url = self._create_upload_session(dest_drive, dest_parent_id, name)
        sent = 0

        status = self._get_session_status(upload_url)
        if status:
            nxt = self._parse_next_start(status)
            if nxt is not None:
                sent = max(sent, nxt)

        while sent < total_size:
            try:
                chunk = self._download_range(src_drive, src_item_id, sent, min(self.CHUNK, total_size - sent))
                resp = self._upload_session_put(upload_url, chunk, sent, total_size, max_tries=12)

                if resp.status_code in (200, 201):
                    return resp
                if resp.status_code == 202:
                    try:
                        st = resp.json()
                    except Exception:
                        st = None
                    nxt = self._parse_next_start(st)
                    if nxt is not None and nxt >= sent:
                        sent = nxt
                    else:
                        sent += len(chunk)
                    continue
                if resp.status_code in (404, 410):
                    upload_url = self._create_upload_session(dest_drive, dest_parent_id, name)
                    sent = 0
                    continue
                resp.raise_for_status()

            except Exception:
                st = self._get_session_status(upload_url)
                if st is None:
                    upload_url = self._create_upload_session(dest_drive, dest_parent_id, name)
                    sent = 0
                else:
                    nxt = self._parse_next_start(st)
                    if nxt is not None and nxt >= sent:
                        sent = nxt

    #Mirroring
    def mirror_files_exact(self, *, src_drive, src_parent="root", dest_drive, dest_parent, root_name, log):
        # pick destination base (blank root_name => use dest_parent directly)
        if root_name:
            dst_root = self.drive.ensure_folder_by_path(dest_drive, dest_parent, root_name)
            base_path = root_name
        else:
            dst_root = dest_parent
            base_path = ""

        # Stack frames:
        #   ("DIR", src_id, dest_id, path)  -> process that folder
        #   ("AFTER", parent_src_id, child_name) -> advance parent's cursor after child processed
        stack = [("DIR", (src_parent or "root"), dst_root, base_path)]

        while stack:
            frame = stack.pop()

            # Post-visit: set cursor for parent after finishing a child folder
            if isinstance(frame, tuple) and frame and frame[0] == "AFTER":
                _, parent_sid, child_name = frame
                self.set_cursor(parent_sid, child_name)
                continue

            # Normal directory frame (back-compat with (sid, did, path))
            if frame and frame[0] == "DIR" and len(frame) == 4:
                _, sid, did, path = frame
            else:
                sid, did, path = frame

            if self.should_cancel():
                return

            log(f"[DIR] {path or '/'}")

            # Build dest file map (only if delete_extras enabled)
            dest_files = self.drive.list_files_map(dest_drive, did) if hasattr(self, "DELETE_EXTRAS") and self.DELETE_EXTRAS else {}

            # Cursor for this source folder
            last = self.get_cursor(sid)

            # Deterministic listing
            url = (
                f"{GRAPH}/drives/{src_drive}/root/children?$top=200&$select=id,name,folder,file,size,hashes&$orderby=name"
                if (sid == "root")
                else f"{GRAPH}/drives/{src_drive}/items/{sid}/children?$top=200&$select=id,name,folder,file,size,hashes&$orderby=name"
            )

            while url:
                if self.should_cancel():
                    return
                j = self.RH.get(url).json()
                for ch in j.get("value", []):
                    nm = ch["name"]

                    # Skip anything <= last processed name (resume fast)
                    if last is not None and nm <= last:
                        if hasattr(self, "DELETE_EXTRAS") and self.DELETE_EXTRAS:
                            dest_files.pop(nm, None)
                        continue

                    # FOLDER child
                    if "folder" in ch:
                        ndid = self.drive.ensure_folder_by_path(dest_drive, did, nm)
                        # Depth-first - only advance parent AFTER this subfolder is fully complete
                        stack.append(("AFTER", sid, nm))
                        stack.append(("DIR", ch["id"], ndid, f"{path+'/'+nm if path else nm}"))
                        continue

                    # FILE child
                    src_size = ch.get("size", 0) or 0
                    src_hash = (ch.get("hashes") or {}).get("quickXorHash")
                    ex = self.drive.try_get_dest_file_fast(dest_drive, did, nm)
                    if ex:
                        _, dst_size, dst_hash = ex
                        same_size = (dst_size == src_size)
                        hashes_known_and_equal = bool(src_hash) and bool(dst_hash) and (src_hash == dst_hash)
                        hashes_both_missing = (not src_hash) and (not dst_hash)

                        if same_size and (hashes_known_and_equal or hashes_both_missing):
                            log(f"  [SKIP] {(path+'/'+nm if path else nm)} (size{' + hash' if hashes_known_and_equal else ' only'})")
                            if hasattr(self, "DELETE_EXTRAS") and self.DELETE_EXTRAS:
                                dest_files.pop(nm, None)
                            self.set_cursor(sid, nm); last = nm
                            continue                    
                    """
                    if ex:
                        _, dst_size, dst_hash = ex
                        if (dst_size == src_size) and (src_hash and dst_hash and src_hash == dst_hash):
                            log(f"  [SKIP] {(path+'/'+nm if path else nm)} (size+hash)")
                            if hasattr(self, "DELETE_EXTRAS") and self.DELETE_EXTRAS:
                                dest_files.pop(nm, None)
                            # advance cursor to this file name
                            self.set_cursor(sid, nm)
                            last = nm
                            continue
                    """     
                    try:
                        self.upload_stream_replace(dest_drive, did, nm, src_drive, ch["id"], src_size)
                        log(f"  [COPY] {(path+'/'+nm if path else nm)} ({src_size} bytes)")
                        if hasattr(self, "DELETE_EXTRAS") and self.DELETE_EXTRAS:
                            dest_files.pop(nm, None)
                        self.set_cursor(sid, nm)
                        last = nm
                    except Exception as e:
                        log(f"  [FAIL] {(path+'/'+nm if path else nm)} -> {e}")

                url = j.get("@odata.nextLink")

            # Finished this folder's own children; any extras left are real extras
            if hasattr(self, "DELETE_EXTRAS") and self.DELETE_EXTRAS and dest_files:
                for nm, (fid, _, _) in dest_files.items():
                    d = self.RH.delete(f"{GRAPH}/drives/{dest_drive}/items/{fid}")
                    if d.status_code not in (200, 204):
                        d.raise_for_status()
                    log(f"  [DELETE] {(path+'/'+nm if path else nm)}")

            # Folder complete — clear its cursor to keep state minimal
            self.clear_cursor(sid)

    def mirror_folders_only(self, *, src_drive, src_parent="root", dest_drive, dest_parent, root_name, log):
        # pick destination base (blank root_name => use dest_parent directly)
        if root_name:
            dst_root = self.drive.ensure_folder_by_path(dest_drive, dest_parent, root_name)
            base_path = root_name
        else:
            dst_root = dest_parent
            base_path = ""

        stack = [(src_parent or "root", dst_root, base_path)]
        while stack:
            if self.should_cancel():
                return

            sid, did, path = stack.pop()
            log(f"[DIR] {path or '/'}")

            # Deterministic listing
            url = (
                f"{GRAPH}/drives/{src_drive}/root/children?$top=200&$select=id,name,folder&$orderby=name"
                if sid == "root"
                else f"{GRAPH}/drives/{src_drive}/items/{sid}/children?$top=200&$select=id,name,folder&$orderby=name"
            )

            while url:
                if self.should_cancel():
                    return
                j = self.RH.get(url).json()
                for ch in j.get("value", []):
                    if "folder" in ch:
                        nm = ch["name"]
                        ndid = self.drive.ensure_folder_by_path(dest_drive, did, nm)
                        stack.append((ch["id"], ndid, f"{path+'/'+nm if path else nm}"))
                url = j.get("@odata.nextLink")
