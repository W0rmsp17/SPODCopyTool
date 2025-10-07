from urllib.parse import quote, urlparse

GRAPH = "https://graph.microsoft.com/v1.0"

def _clean(name: str) -> str:
    return name.replace("/", "_").strip() or "_"

def _enc(name: str) -> str:
    return quote(_clean(name), safe="")

def _parse_site_url(url: str):
    u = urlparse(url)
    host = u.netloc
    path = u.path or "/"
    if not host:
        raise ValueError("Invalid SharePoint site URL (missing host)")
    return host, path.rstrip("/")
#

