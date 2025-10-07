import time, random, email.utils, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone

#status class
CODES = {
    "OK":    {200, 201, 202, 204, 206},
    "RETRY": {408, 416, 425, 429, 500, 502, 503, 504},
    "AUTH":  {401, 403},
}
def is_ok(s): return s in CODES["OK"]
def is_retry(s): return s in CODES["RETRY"]
def is_auth(s): return s in CODES["AUTH"]

#Backoff helpers
def _sleep(attempt: int):
    # 
    time.sleep((attempt + 1) * 0.8 + random.random() * 0.3)

def _parse_retry_after(header_val):
    if header_val is None:
        return None
    try:
        return float(header_val)  # seconds
    except Exception:
        try:
            dt = email.utils.parsedate_to_datetime(header_val)
            if dt is None:
                return None
            now = datetime.now(dt.tzinfo or timezone.utc)
            return max(0.0, (dt - now).total_seconds())
        except Exception:
            return None

def _sleep_with_retry_after(resp, attempt: int):
    ra = _parse_retry_after(resp.headers.get("Retry-After"))
    if ra and ra > 0:
        time.sleep(ra)
    else:
        _sleep(attempt)

# Session factory 
def new_session():
    s = requests.Session()
    # AHandle Retry connection/read errors aggressively. logic handles status code retries
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        allowed_methods=None,   # retry all methods on conn/read err
        raise_on_status=False,  # we handle status ourselves
        respect_retry_after_header=True,
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", ad); s.mount("http://", ad)
    s.headers.update({"User-Agent": "MoveObject/GraphCopyTool"})
    return s

# wrapper 
class RobustHTTP:
    def __init__(self, session, get_auth_hdr, timeout=(10, 300), refresh_cb_default=None,
                 on_throttle=None):
        self.S = session
        self.get_auth_hdr = get_auth_hdr
        self.timeout = timeout
        self.refresh_cb_default = refresh_cb_default
        self.on_throttle = on_throttle  

    def _merged_headers(self, headers):
        base = self.get_auth_hdr() or {}
        if headers:
            base.update(headers)
        return base

    def _request(
        self, method, url, *,
        headers=None, params=None, data=None, json=None,
        allow_redirects=True, stream=False,
        max_tries=6, refresh_cb=None,
        ok_extra: set | tuple = (),
    ):
        """
        Two-phase retry:
          Phase A: try max_tries with current token
          If AUTH encountered or we exhaust: run refresh_cb once
          Phase B: try max_tries with refreshed token
        Retries also on RETRY status set and request exceptions
        """
        if refresh_cb is None:
            refresh_cb = self.refresh_cb_default

        def _once():
            return self.S.request(
                method, url,
                headers=self._merged_headers(headers),
                params=params, data=data, json=json,
                timeout=self.timeout,
                allow_redirects=allow_redirects,
                stream=stream,
            )

        # Phase A
        for a in range(max_tries):
            try:
                r = _once()
                code = r.status_code
                if is_ok(code) or (ok_extra and code in ok_extra):
                    return r
                if is_retry(code):
                    if self.on_throttle and code in (429, 502, 503, 504):
                        ra = _parse_retry_after(r.headers.get("Retry-After"))
                        try: self.on_throttle(code, ra)
                        except Exception: pass
                    _sleep_with_retry_after(r, a); continue
                if is_auth(code):
                    break  # go refresh once
                r.raise_for_status()
                return r  # In case a 2xx not in OK set slipped through
            except requests.RequestException:
                _sleep(a)

        # Refresh once (if provided)
        if refresh_cb:
            try:
                refresh_cb()
            except Exception:
                # If refresh itself fails, fall through to Phase B attempts anyway
                pass

        # Phase B
        for b in range(max_tries):
            try:
                r = _once()
                code = r.status_code
                if is_ok(code) or (ok_extra and code in ok_extra):
                    return r
                if is_retry(code):
                    if self.on_throttle and code in (429, 502, 503, 504):
                        ra = _parse_retry_after(r.headers.get("Retry-After"))
                        try: self.on_throttle(code, ra)
                        except Exception: pass
                    _sleep_with_retry_after(r, b); continue
                r.raise_for_status()
                return r
            except requests.RequestException:
                _sleep(b)
        msg = f"{method} failed after retries: {url}"
        try:
            # small safeguard: if r exists, surface a hint
            if 'r' in locals():
                snippet = r.text[:512].replace("\n", " ")
                msg += f" (last status={r.status_code}, body={snippet!r})"
        except Exception:
            pass
        raise RuntimeError(msg)

    #public surface kept compatible
    def get(self, url, headers=None, params=None, allow_redirects=True, max_tries=6,
            refresh_cb=None, ok_extra: set | tuple = (), stream=False):
        return self._request(
            "GET", url,
            headers=headers, params=params, allow_redirects=allow_redirects,
            max_tries=max_tries, refresh_cb=refresh_cb, ok_extra=ok_extra, stream=stream
        )


    def post(self, url, *, headers=None, data=None, json=None,
             max_tries=8, refresh_cb=None):
        return self._request(
            "POST", url,
            headers=headers, data=data, json=json,
            max_tries=max_tries, refresh_cb=refresh_cb
        )

    def put(self, url, headers=None, data=None, max_tries=10, refresh_cb=None, stream=False):
        return self._request(
            "PUT", url,
            headers=headers, data=data, stream=stream,
            max_tries=max_tries, refresh_cb=refresh_cb
        )

    def delete(self, url, *, headers=None, max_tries=6, refresh_cb=None):
        return self._request(
            "DELETE", url,
            headers=headers,
            max_tries=max_tries, refresh_cb=refresh_cb
        )

    # handy extras if needed
    def patch(self, url, *, headers=None, data=None, json=None,
              max_tries=8, refresh_cb=None):
        return self._request(
            "PATCH", url,
            headers=headers, data=data, json=json,
            max_tries=max_tries, refresh_cb=refresh_cb
        )

    def head(self, url, *, headers=None, max_tries=6, refresh_cb=None):
        return self._request(
            "HEAD", url,
            headers=headers, allow_redirects=False,
            max_tries=max_tries, refresh_cb=refresh_cb
        )
