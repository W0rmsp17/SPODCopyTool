from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from threading import BoundedSemaphore
from graph_client.graph_common import GRAPH, _enc

class TransferManager:
    def __init__(self, http, drive_client, *,
                 chunk=8*1024*1024, min_chunk=1*1024*1024, max_single=4*1024*1024,
                 get_cursor=None, set_cursor=None, clear_cursor=None, should_cancel=None,
                 on_discover_file=None, on_file_done=None,
                 start_concurrency=2, max_concurrency=4, min_concurrency=1):

        self.RH = http
        self.drive = drive_client
        self.CHUNK = int(chunk)
        self.MIN_CHUNK = int(min_chunk)
        self.MAX_SINGLE = int(max_single)

        # resume/cancel
        self.get_cursor = get_cursor or (lambda folder_id: None)
        self.set_cursor = set_cursor or (lambda folder_id, name: None)
        self.clear_cursor = clear_cursor or (lambda folder_id: None)
        self.should_cancel = should_cancel or (lambda: False)

        # stats hooks
        self.on_discover_file = on_discover_file or (lambda size=0: None)
        self.on_file_done     = on_file_done     or (lambda size=0: None)

        # concurrency controls
        self._conc_min = int(min_concurrency)
        self._conc_max = int(max_concurrency)
        self._target_capacity = max(self._conc_min, min(int(start_concurrency), self._conc_max))

        # executor has room up to max; semaphore gates EFFECTIVE concurrency
        self._executor = ThreadPoolExecutor(max_workers=self._conc_max, thread_name_prefix="xfer")
        self._sem = BoundedSemaphore(value=self._conc_max)

        # start at target_capacity by pre-consuming permits
        for _ in range(self._conc_max - self._target_capacity):
            self._sem.acquire()

        # DELETE_EXTRAS is set by controller (optional)
        if not hasattr(self, "DELETE_EXTRAS"):
            self.DELETE_EXTRAS = False

    # AIMD hooks 
    def concurrency(self) -> int:
        # report target capacity (what AIMD is steering)
        return self._target_capacity

    def scale_up(self) -> bool:
        """Increase allowed concurrency by 1 (if below max)."""
        if self._target_capacity >= self._conc_max:
            return False
        try:
            self._sem.release()  
            self._target_capacity += 1
            return True
        except ValueError:
            return False

    def scale_down(self) -> bool:
        """Decrease allowed concurrency by 1 (non-blocking; won't reduce below min)."""
        if self._target_capacity <= self._conc_min:
            return False
        ok = self._sem.acquire(blocking=False)  # only succeeds if a free permit exists
        if ok:
            self._target_capacity -= 1
        return ok

    def shutdown(self):
        self._executor.shutdown(wait=False, cancel_futures=False)

    # ---------- schedule gated copy ----------
    def _submit_copy(self, *, dest_drive, did, nm, src_drive, item_id, src_size, sid, path, log):
        # acquire a capacity permit before starting
        self._sem.acquire()

        def _job():
            try:
                self.upload_stream_replace(dest_drive, did, nm, src_drive, item_id, src_size)
                log(f"  [COPY] {(path+'/'+nm if path else nm)} ({src_size} bytes)")
                try: self.set_cursor(sid, nm)
                except Exception: pass
                try: self.on_file_done(src_size)
                except Exception: pass
            except Exception as e:
                log(f"  [FAIL] {(path+'/'+nm if path else nm)} -> {e}")
            finally:
                # always release so another job can start
                try: self._sem.release()
                except ValueError: pass

        return self._executor.submit(_job)

    # downloads/uploads
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

    # mirroring 
    def mirror_files_exact(self, *, src_drive, src_parent="root", dest_drive, dest_parent, root_name, log):
        if root_name:
            dst_root = self.drive.ensure_folder_by_path(dest_drive, dest_parent, root_name)
            base_path = root_name
        else:
            dst_root = dest_parent
            base_path = ""

        # frames: ("DIR", sid, did, path) and ("AFTER", parent_sid, child_name)
        stack = [("DIR", (src_parent or "root"), dst_root, base_path)]

        while stack:
            frame = stack.pop()

            if isinstance(frame, tuple) and frame and frame[0] == "AFTER":
                _, parent_sid, child_name = frame
                self.set_cursor(parent_sid, child_name)
                continue

            if frame and frame[0] == "DIR" and len(frame) == 4:
                _, sid, did, path = frame
            else:
                sid, did, path = frame

            if self.should_cancel():
                return

            log(f"[DIR] {path or '/'}")

            # DELETE_EXTRAS support
            dest_files = self.drive.list_files_map(dest_drive, did) if self.DELETE_EXTRAS else {}

            last = self.get_cursor(sid)
            url = (
                f"{GRAPH}/drives/{src_drive}/root/children?$top=200&$select=id,name,folder,file,size,hashes&$orderby=name"
                if (sid == "root")
                else f"{GRAPH}/drives/{src_drive}/items/{sid}/children?$top=200&$select=id,name,folder,file,size,hashes&$orderby=name"
            )

            folder_futs = []

            while url:
                if self.should_cancel():
                    return
                j = self.RH.get(url).json()
                for ch in j.get("value", []):
                    nm = ch["name"]

                    # resume fast
                    if last is not None and nm <= last:
                        if self.DELETE_EXTRAS:
                            dest_files.pop(nm, None)
                        continue

                    if "folder" in ch:
                        ndid = self.drive.ensure_folder_by_path(dest_drive, did, nm)
                        stack.append(("AFTER", sid, nm))
                        stack.append(("DIR", ch["id"], ndid, f"{path+'/'+nm if path else nm}"))
                        continue

                    # file
                    src_size = ch.get("size", 0) or 0
                    src_hash = (ch.get("hashes") or {}).get("quickXorHash")

                    #try: self.on_discover_file(sr _size)
                    try: self.on_discover_file(1) 
                    except Exception: pass

                    ex = self.drive.try_get_dest_file_fast(dest_drive, did, nm)
                    if ex:
                        _, dst_size, dst_hash = ex
                        same_size = (dst_size == src_size)
                        hashes_known_and_equal = bool(src_hash) and bool(dst_hash) and (src_hash == dst_hash)
                        hashes_both_missing = (not src_hash) and (not dst_hash)

                        if same_size and (hashes_known_and_equal or hashes_both_missing):
                            log(f"  [SKIP] {(path+'/'+nm if path else nm)} (size{' + hash' if hashes_known_and_equal else ' only'})")
                            try: self.on_file_done(src_size)
                            except Exception: pass
                            if self.DELETE_EXTRAS:
                                dest_files.pop(nm, None)
                            self.set_cursor(sid, nm); last = nm
                            continue

                    fut = self._submit_copy(
                        dest_drive=dest_drive, did=did, nm=nm,
                        src_drive=src_drive, item_id=ch["id"], src_size=src_size,
                        sid=sid, path=path, log=log
                    )
                    folder_futs.append(fut)
                    if self.DELETE_EXTRAS:
                        dest_files.pop(nm, None)

                url = j.get("@odata.nextLink")

            if folder_futs:
                wait(folder_futs, return_when=FIRST_EXCEPTION)  # job logs handle exceptions

            if self.DELETE_EXTRAS and dest_files:
                for nm, (fid, _, _) in dest_files.items():
                    d = self.RH.delete(f"{GRAPH}/drives/{dest_drive}/items/{fid}")
                    if d.status_code not in (200, 204):
                        d.raise_for_status()
                    log(f"  [DELETE] {(path+'/'+nm if path else nm)}")

            self.clear_cursor(sid)

    def mirror_folders_only(self, *, src_drive, src_parent="root", dest_drive, dest_parent, root_name, log):
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
