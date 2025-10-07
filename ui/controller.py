
#import os
import json
import time
from threading import Thread, Event, Lock
from urllib.parse import quote
from ui.state_store import StateStore, default_state_dir
import msal

from http_utils.http_utils import new_session, RobustHTTP
from graph_client import GraphClient

from graph_client.graph_common import GRAPH

import time
from threading import Lock



class Stats:
    def __init__(self):
        self._lk = Lock()
        self._started_at = None       
        self._finished_at = None
        self.files_total = 0
        self.files_done = 0
        self.bytes_done = 0
        self.current_workers = 1
        self.throttles_recent = 0

    #internal helpers
    def _ensure_started(self):
        if self._started_at is None:
            self._started_at = time.time()

    #counters (start timer on first real work)
    def on_discover_file(self, n=1):
        with self._lk:
            self._ensure_started()
            self.files_total += int(n)

    def on_file_done(self, size=0):
        with self._lk:
            self._ensure_started()
            self.files_done += 1
            self.bytes_done += int(size)

    def set_workers(self, n):
        with self._lk:
            self.current_workers = int(n)

    def on_throttle(self, *_args, **_kw):
        with self._lk:
            self.throttles_recent += 1

    def reset_throttle_window(self):
        with self._lk:
            self.throttles_recent = 0

    #ifecycle (optional external calls)
    def finish(self):
        with self._lk:
            self._finished_at = time.time()

    #snapshot
    def snapshot(self):
        with self._lk:
            now = time.time()
            if self._started_at is None:
                elapsed = 0
            else:
                elapsed = int((self._finished_at or now) - self._started_at)
            rate = (self.bytes_done / elapsed) if elapsed > 0 else 0
            return {
                "files_total": self.files_total,
                "files_done":  self.files_done,
                "elapsed":     elapsed,
                "rate":        rate,
                "workers":     self.current_workers,
                "throttles_recent": self.throttles_recent,
            }


    
    def on_discover_file(self, n=1):
        with self._lk:
            self.files_total += n

    def on_file_done(self, size=0):
        with self._lk:
            self.files_done += 1
            self.bytes_done += int(size)

    def set_workers(self, n):
        with self._lk:
            self.current_workers = int(n)

    def add_throttles(self, n=1):
        with self._lk:
            self.throttles_recent += int(n)

    def drain_throttles(self):
        with self._lk:
            n = self.throttles_recent
            self.throttles_recent = 0
            return n

class Controller:
    def __init__(self, *, timeout, chunk, min_chunk, max_single, delete_extras):
        
        self.TIMEOUT = timeout
        self.CHUNK = chunk
        self.MIN_CHUNK = min_chunk
        self.MAX_SINGLE = max_single
        self.DELETE_EXTRAS = delete_extras
        #state feilds
        self._state_sig = None
        self._state = None
        # rumtime
        self.S = None
        self.RH = None
        self.client = None
        
        self.stats = Stats()

        # Authentication
        self.TENANT = self.CLIENT = self.SECRET = ""
        self._T = None

        # Cancelation + stage
        self.CANCEL_EV = Event()
        self._stage_text = "idle"

        # callbacks (set by App)
        self._log = None
        self._set_stage = None

        # lookups
        self.SRC_SITES = {}
        self.DST_SITES = {}
        self.SRC_LIBS = {}
        self.DST_LIBS = {}
        self.SRC_PARENTS = {}
        self.DST_PARENTS = {}
        self.DST_USERS = {}

        # current selection (for convenience)
        self.SRC_DRIVE = None
        self.SRC_PARENT = None
        self.DEST_DRIVE = None
        self.DEST_PARENT = None
        self.ROOT_NAME = "SRC_ROOT" #byDefault

        self.state = StateStore(base_dir=default_state_dir("SPODCopyTool"))
        self.log(f"[RESUME] Using state dir: {self.state.base_dir}")
    
    def get_stats(self):
        return self.stats.snapshot()    
    
    def _ensure_state(self):
        sig = self._job_signature()
        st = self.state.load(sig)
        if not st:
            st = {"phase": "folders", "folder_cursors": {}}
        self._state_sig = sig
        self._state = st

    def _save_state(self):
        if self._state_sig and self._state is not None:
            self.state.save(self._state_sig, self._state)

    def _clear_state(self):
        if self._state_sig:
            self.state.clear(self._state_sig)
        self._state = None

    def _job_signature(self) -> dict:
            return {
                "tenant": self.TENANT or "",
                "src_drive": self.SRC_DRIVE or "",
                "src_parent": (self.SRC_PARENT or "root"),
                "dest_drive": self.DEST_DRIVE or "",
                "dest_parent": self.DEST_PARENT or "",
                "root_name": self.ROOT_NAME or "",
            }

    def set_callbacks(self, *, log, set_stage):
        self._log = log
        self._set_stage = set_stage

    def log(self, msg: str):
        if self._log:
            self._log(msg)

    def stage(self, text: str):
        self._stage_text = text
        if self._set_stage:
            self._set_stage(text)

    def stage_ok(self):
        self._stage_text = f"{self._stage_text}  ✔"
        if self._set_stage:
            self._set_stage(self._stage_text)

    def stage_fail(self):
        self._stage_text = f"{self._stage_text}  ✖"
        if self._set_stage:
            self._set_stage(self._stage_text)

    def token(self):
        app = msal.ConfidentialClientApplication(
            self.CLIENT,
            authority=f"https://login.microsoftonline.com/{self.TENANT}",
            client_credential=self.SECRET,
        )
        r = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" not in r:
            raise RuntimeError(json.dumps(r, indent=2))
        return r["access_token"]

    def get_token(self):
        if self._T is None:
            self._T = self.token()
        return self._T

    def reset_token(self):
        self._T = None

    def Hdyn(self):
        return {"Authorization": f"Bearer {self.get_token()}"}

    #cursor + cancel hooks used by TransferManager
    def _cursor_get(self, folder_id):
        return (self._state or {}).get("folder_cursors", {}).get(folder_id)

    def _cursor_set(self, folder_id, name):
        if self._state is None:
            return
        fc = self._state.setdefault("folder_cursors", {})
        prev = fc.get(folder_id)
        if prev is None or name > prev:             
            fc[folder_id] = name
            cnt = getattr(self, "_cursor_updates", 0) + 1
            if cnt % 50 == 0:
                self._save_state()
            self._cursor_updates = cnt

    def _cursor_clear(self, folder_id):
        if self._state is None:
            return
        self._state.get("folder_cursors", {}).pop(folder_id, None)
        self._save_state()

    def _should_cancel(self):
        return self.CANCEL_EV.is_set()

    def lazy_init(self):
        if self.client is not None:
            return
        self.S = new_session()
        self.RH = RobustHTTP(
            self.S,
            get_auth_hdr=self.Hdyn,
            timeout=self.TIMEOUT,
            refresh_cb_default=self.reset_token,
            on_throttle=self.stats.on_throttle,  
        )

        self.client = GraphClient(
            http=self.RH,
            reset_token=self.reset_token,
            timeout=self.TIMEOUT,
            chunk=self.CHUNK, min_chunk=self.MIN_CHUNK, max_single=self.MAX_SINGLE,
            delete_extras=self.DELETE_EXTRAS,
        )

        try:
            self.client.xfer.on_discover_file = self.stats.on_discover_file
            self.client.xfer.on_file_done     = self.stats.on_file_done
        except Exception:
            pass

        x = self.client.xfer
        x.get_cursor    = self._cursor_get
        x.set_cursor    = self._cursor_set
        x.clear_cursor  = self._cursor_clear
        x.should_cancel = self._should_cancel
        x.DELETE_EXTRAS = self.DELETE_EXTRAS

        # stats hooks
        x.on_discover_file = self.stats.on_discover_file
        x.on_file_done     = self.stats.on_file_done

    def connect(self, *, tenant, client, secret):
        self.TENANT, self.CLIENT, self.SECRET = tenant.strip(), client.strip(), secret.strip()
        self._T = None
        self.lazy_init()
        sites = self.client.search_sites("*")
        names = []
        self.SRC_SITES.clear(); self.DST_SITES.clear()
        for s in sites:
            nm = s["name"]
            self.SRC_SITES[nm] = s["id"]
            self.DST_SITES[nm] = s["id"]
            names.append(nm)
        names.sort(key=str.lower)
        return names

    def select_src_site(self, name):
        sid = self.SRC_SITES.get(name)
        if not sid:
            return []
        libs = self.client.list_site_libraries(sid)
        self.SRC_LIBS.clear()
        for lib in libs:
            self.SRC_LIBS[lib["name"]] = lib["id"]
        lib_names = sorted(self.SRC_LIBS.keys(), key=str.lower)
        return lib_names

    def select_src_lib(self, libname):
        did = self.SRC_LIBS.get(libname)
        if not did:
            return {"drive_id": None, "parent_names": []}
        self.SRC_DRIVE = did
        self.SRC_PARENT = "root"
        folders = self.client.list_folders(did, "root")
        self.SRC_PARENTS.clear()
        self.SRC_PARENTS["(root)"] = "root"
        for nm, fid in folders:
            self.SRC_PARENTS[nm] = fid
        names = list(self.SRC_PARENTS.keys())
        return {"drive_id": did, "parent_names": names, "default_parent_id": "root"}
    
    def select_src_parent(self, label: str):
        fid = self.SRC_PARENTS.get(label)
        if fid:
            self.SRC_PARENT = fid
        return self.SRC_PARENT

    def select_dst_site(self, name):
        sid = self.DST_SITES.get(name)
        if not sid:
            return []
        libs = self.client.list_site_libraries(sid)
        self.DST_LIBS.clear()
        for lib in libs:
            self.DST_LIBS[lib["name"]] = lib["id"]
        names = sorted(self.DST_LIBS.keys(), key=str.lower)
        return names

    def select_dst_lib(self, libname):
        did = self.DST_LIBS.get(libname)
        if not did:
            return {"drive_id": None, "parent_names": []}
        self.DEST_DRIVE = did
        root_id = self.client.get_drive_root_id(did)
        self.DEST_PARENT = root_id
        folders = self.client.list_folders(did, "root")
        self.DST_PARENTS.clear()
        self.DST_PARENTS["(root)"] = root_id
        for nm, fid in folders:
            self.DST_PARENTS[nm] = fid
        names = list(self.DST_PARENTS.keys())
        return {"drive_id": did, "parent_names": names, "default_parent_id": root_id}

    def search_users(self, query):
        q = (query or "").strip()
        if not q:
            return []
        users = self.client.search_users(q, top=25)
        self.DST_USERS.clear()
        display = []
        for u in users:
            label = f"{u['name']}  <{u['upn']}>"
            self.DST_USERS[label] = u["upn"] or u["id"]
            display.append(label)
        return display

    def choose_user(self, label):
        key = self.DST_USERS.get(label)
        if not key:
            return None
        resolved = self.client.resolve_user_drive(key)
        if not resolved:
            return None
        drive_id, _ = resolved
        self.DEST_DRIVE = drive_id
        root_id = self.client.get_drive_root_id(drive_id)
        self.DEST_PARENT = root_id
        folders = self.client.list_folders(drive_id, "root")
        self.DST_PARENTS.clear()
        for nm, fid in folders:
            self.DST_PARENTS[nm] = fid
        return {
            "drive_id": drive_id,
            "root_id": root_id,
            "parent_names": list(self.DST_PARENTS.keys()),
            "first_parent_id": folders[0][1] if folders else root_id,
        }
    #---------------------
    #   job lifecycle
    #---------------------
    def start_job(self, cfg: dict):
        # cfg keys: SRC_DRIVE, SRC_PARENT, DEST_DRIVE, DEST_PARENT, ROOT_NAME, TENANT, CLIENT, SECRET
        self.SRC_DRIVE  = cfg.get("SRC_DRIVE", "").strip()
        self.SRC_PARENT = (cfg.get("SRC_PARENT") or "root").strip()
        self.DEST_DRIVE = cfg.get("DEST_DRIVE", "").strip()
        self.DEST_PARENT= cfg.get("DEST_PARENT", "").strip()
        self.ROOT_NAME  = cfg.get("ROOT_NAME", "").strip()
        self.TENANT     = cfg.get("TENANT", "").strip()
        self.CLIENT     = cfg.get("CLIENT", "").strip()
        self.SECRET     = cfg.get("SECRET", "").strip()
        self._T = None

        self._ensure_state()
        phase = self._state.get("phase")
        self.log(f"[RESUME] Phase = {phase}")

        self._target_workers = 2

        # reset stats fresh for this run
        self.stats = Stats()
        self.stats.set_workers(self._target_workers)

        # rebind hooks if client already exists
        if self.client is not None:
            x = self.client.xfer
            x.on_discover_file = self.stats.on_discover_file
            x.on_file_done     = self.stats.on_file_done

        self._start_aimd_loop_once()
        job_sig = {
            "src_drive":   cfg.get("SRC_DRIVE"),
            "src_parent":  cfg.get("SRC_PARENT"),
            "dest_drive":  cfg.get("DEST_DRIVE"),
            "dest_parent": cfg.get("DEST_PARENT"),
            "root":        cfg.get("ROOT_NAME"),
        }
        try:
            self.state.ensure_fresh_state(job_sig)
        except Exception as e:
            self.log(f"[STATE] ensure_fresh_state error: {e}")

        # runner thread
        def _runner():
            try:
                self.lazy_init()
            except Exception as e:
                self.log(f"[INIT-ERROR] {e}")
                return
            self._run_job()

        Thread(target=_runner, daemon=True).start()


    def _start_aimd_loop_once(self):
        if getattr(self, "_aimd_thread", None) and self._aimd_thread.is_alive():
            return
        self._aimd_stop = False

        def _aimd():
            while not self._aimd_stop and not self.CANCEL_EV.is_set():
                time.sleep(20)
                snap = self.stats.snapshot()
                thr = snap.get("throttles_recent", 0)

                # safe no-ops if xfer lacks these methods
                xu = getattr(self.client.xfer, "scale_up",  lambda: False)
                xd = getattr(self.client.xfer, "scale_down",lambda: False)

                if thr >= 2:
                    if xd():
                        self._target_workers = max(1, self._target_workers - 1)
                        self.stats.set_workers(self._target_workers)
                        self.log("[AIMD] throttle -> scale DOWN")
                elif thr == 0 and snap["files_done"] < snap["files_total"]:
                    if xu():
                        self._target_workers = min(4, self._target_workers + 1)
                        self.stats.set_workers(self._target_workers)
                        self.log("[AIMD] stable -> scale UP")

                self.stats.reset_throttle_window()

        t = Thread(target=_aimd, name="_aimd", daemon=True)
        t.start()
        self._aimd_thread = t

    def cancel_job(self):
        self.CANCEL_EV.set()
        # stop AIMD loop if running
        try:
            self._aimd_stop = True
            if getattr(self, "_aimd_thread", None):
                self._aimd_thread.join(timeout=0.5)
        except Exception:
            pass
        self._finished_at = time.time()
        self._save_state()
        self.log("[CANCEL] Requested. Will stop after current step.")

    #  the work
    def _run_job(self):
        try:
            _ = self.get_token()
            self.CANCEL_EV.clear()

            if self._state.get("phase") == "folders":
                self.stage("mirroring folder structure")
                self.log("########################")
                self.log("#-FILES MIRROR STARTED-#")
                self.log("########################")
                self.client.mirror_folders_only(
                    src_drive=self.SRC_DRIVE,
                    src_parent=(self.SRC_PARENT or "root"),
                    dest_drive=self.DEST_DRIVE,
                    dest_parent=self.DEST_PARENT,
                    root_name=self.ROOT_NAME,
                    log=self.log,
                )
                self.stage_ok()
                if self.CANCEL_EV.is_set():
                    self.stage("cancelled"); self._save_state()
                    return

                self._state["phase"] = "files"; self._save_state()

            if self._state.get("phase") == "files":
                self.stage("copying files to destination")
                self.client.mirror_files_exact(
                    src_drive=self.SRC_DRIVE,
                    src_parent=(self.SRC_PARENT or "root"),
                    dest_drive=self.DEST_DRIVE,
                    dest_parent=self.DEST_PARENT,
                    root_name=self.ROOT_NAME,
                    log=self.log,
                )
                self.stage_ok()
                if self.CANCEL_EV.is_set():
                    self.stage("cancelled"); self._save_state()
                    return

                self._state["phase"] = "audit"; self._save_state()

            if self._state.get("phase") == "audit":
                self.stage("post job audit")
                self._audit_pass(
                    src_drive=self.SRC_DRIVE,
                    src_parent=(self.SRC_PARENT or "root"),
                    dest_drive=self.DEST_DRIVE,
                    dest_parent=self.DEST_PARENT,
                    root_name=self.ROOT_NAME,
                )
                self.stage_ok()
                self.log("FILES MIRRORED")
                self._clear_state()

        except Exception as e:
            self.stage_fail()
            self.log(f"[FATAL] {type(e).__name__}: {e}")

        finally:
            # stop AIMD + finish stats no matter what
            try:
                self._aimd_stop = True
                if getattr(self, "_aimd_thread", None):
                    self._aimd_thread.join(timeout=0.5)
            except Exception:
                pass
            try:
                self.stats.finish()
            except Exception:
                pass


    def _audit_pass(self, *, src_drive, src_parent, dest_drive, dest_parent, root_name):
        total_src = total_dst = 0
        matched = mismatched = missing = 0

        
        if root_name:
            r = self.RH.get(
                f"{GRAPH}/drives/{dest_drive}/items/{dest_parent}:/{quote(root_name, safe='')}:",
                ok_extra=(404,),
            )
            if r.status_code == 404:
                self.log("[AUDIT] Destination root missing; all files deemed missing.")
                dst_root_id = None
            else:
                r.raise_for_status()
                dst_root_id = r.json()["id"]
        else:
            dst_root_id = dest_parent

        stack = [(src_parent or "root", dst_root_id, root_name or "")]
        while stack and not self.CANCEL_EV.is_set():
            sid, did, path = stack.pop()
            self.log(f"[AUDIT] {path or '/'}")

            # Build dest file map only if dest folder exists 
            
            dest_files = {}
            if did:
                dest_files = self.client.list_files_map(dest_drive, did)

            url = (
                f"{GRAPH}/drives/{src_drive}/root/children?$top=200&$select=id,name,folder,file,size,hashes&$orderby=name"
                if sid == "root" else
                f"{GRAPH}/drives/{src_drive}/items/{sid}/children?$top=200&$select=id,name,folder,file,size,hashes&$orderby=name"
            )


            while url and not self.CANCEL_EV.is_set():
                j = self.RH.get(url).json()
                for ch in j.get("value", []):
                    nm = ch["name"]
                    rel = f"{path+'/'+nm if path else nm}"

                    if "folder" in ch:
                        ndid = None
                        if did:
                            r = self.RH.get(
                                f"{GRAPH}/drives/{dest_drive}/items/{did}:/{quote(nm, safe='')}:?$select=id,folder",
                                ok_extra=(404,),
                            )
                            if r.status_code == 200 and "folder" in r.json():
                                ndid = r.json()["id"]
                        stack.append((ch["id"], ndid, rel))
                        continue

                    # files
                    total_src += 1
                    src_size = ch.get("size", 0) or 0
                    src_hash = (ch.get("hashes") or {}).get("quickXorHash")

                    if did:
                        ex = self.client.try_get_dest_file_fast(dest_drive, did, nm)
                    else:
                        ex = None

                    if ex:
                        total_dst += 1
                        _, dst_size, dst_hash = ex
                        same_size = (dst_size == src_size)
                        hashes_known_and_equal = bool(src_hash) and bool(dst_hash) and (src_hash == dst_hash)
                        hashes_both_missing = (not src_hash) and (not dst_hash)

                        if same_size and (hashes_known_and_equal or hashes_both_missing):
                            matched += 1
                        else:
                            mismatched += 1

                        """
                        if dst_size == src_size and (src_hash and dst_hash and src_hash == dst_hash):
                            matched += 1
                        else:
                            mismatched += 1
                            self.log(f"  [AUDIT:MISMATCH] {rel} (src {src_size}/{src_hash} vs dst {dst_size}/{dst_hash})")
                        """    
                    else:
                        missing += 1
                        self.log(f"  [AUDIT:MISSING]   {rel}")

                url = j.get("@odata.nextLink")

        summary = (
            f"[AUDIT:SUMMARY] src_files={total_src}, dst_files_seen={total_dst}, "
            f"matched={matched}, mismatched={mismatched}, missing={missing}"
        )
        self.log(summary)
        return {"src": total_src, "dst": total_dst, "matched": matched, "mismatched": mismatched, "missing": missing}
