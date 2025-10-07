from __future__ import annotations

from .drive_client import DriveClient
from .directory_client import DirectoryClient
from .transfer_manager import TransferManager
from .graph_common import GRAPH, _enc


class GraphClient:
    def __init__(
        self, *,
        http, reset_token,
        timeout=(10,300), chunk=8*1024*1024, min_chunk=1*1024*1024, max_single=4*1024*1024,
        delete_extras=False,
        get_cursor=None, set_cursor=None, clear_cursor=None, should_cancel=None,
        on_discover_file=None, on_file_done=None,
    ):
        self.RH = http
        self.reset_token = reset_token
        self.TIMEOUT = timeout
        self.DELETE_EXTRAS = delete_extras

        self.drive = DriveClient(http)
        self.dir   = DirectoryClient(http)
        self.xfer  = TransferManager(
            http=self.RH,
            drive_client=self.drive,
            chunk=chunk, min_chunk=min_chunk, max_single=max_single,
            get_cursor=get_cursor, set_cursor=set_cursor, clear_cursor=clear_cursor,
            should_cancel=should_cancel,
            on_discover_file=on_discover_file,
            on_file_done=on_file_done,
        )
        self.xfer.DELETE_EXTRAS = delete_extras


    #Directory and search passthrough
    def resolve_site_id_from_url(self, *a, **k): return self.dir.resolve_site_id_from_url(*a, **k)
    def list_site_libraries(self, *a, **k):      return self.dir.list_site_libraries(*a, **k)
    def search_sites(self, *a, **k):             return self.dir.search_sites(*a, **k)
    def search_users(self, *a, **k):             return self.dir.search_users(*a, **k)
    def resolve_user_drive(self, *a, **k):       return self.dir.resolve_user_drive(*a, **k)

    #Drive primitives passthrough
    def ensure_folder_by_path(self, *a, **k): return self.drive.ensure_folder_by_path(*a, **k)
    def list_folders(self, *a, **k):          return self.drive.list_folders(*a, **k)
    def list_files_map(self, *a, **k):        return self.drive.list_files_map(*a, **k)
    def get_drive_root_id(self, *a, **k):     return self.drive.get_drive_root_id(*a, **k)
    def try_get_dest_file_fast(self, *a, **k):return self.drive.try_get_dest_file_fast(*a, **k)

    #transfer passthrough
    def upload_stream_replace(self, *a, **k): return self.xfer.upload_stream_replace(*a, **k)
    def mirror_files_exact(self, *a, **k):    return self.xfer.mirror_files_exact(*a, **k)
    def mirror_folders_only(self, *a, **k):   return self.xfer.mirror_folders_only(*a, **k)
