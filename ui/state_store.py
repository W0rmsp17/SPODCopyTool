from __future__ import annotations
import os, sys, json, hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

__all__ = ["StateStore", "default_state_dir"]

def default_state_dir(app_name: str = "SPODCopyTool") -> str:
    """Return a per-OS state directory (override with SPOD_STATE_DIR)."""
    override = os.environ.get("SPOD_STATE_DIR")
    if override:
        return override

    home = Path.home()

    if os.name == "nt":  # Windows
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or (home / "AppData" / "Local")
        return str(Path(base) / app_name / "state")

    if sys.platform == "darwin":  # macOS
        return str(home / "Library" / "Application Support" / app_name / "state")

    # Linux / others
    xdg = os.environ.get("XDG_STATE_HOME")
    if xdg:
        return str(Path(xdg) / app_name)
    return str(home / ".local" / "state" / app_name)


class StateStore:
    """Minimal persisted state for checkpoint/resume.
    Stores per-job JSON under base_dir using a hashed signature-based filename.
    """
    VERSION = 1

    def __init__(self, base_dir: str = ".state"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._current_sig = None

    def _hash_signature(self, signature: Dict[str, Any]) -> str:
        payload = json.dumps(signature, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha1(payload).hexdigest()  # filename id is fine with sha1

    def _path_for(self, signature: Dict[str, Any]) -> Path:
        h = self._hash_signature(signature)[:12]
        return self.base_dir / f"job-{h}.json"
    
    def ensure_fresh_state(self, signature: Dict[str, Any]) -> str:
            """
            Make this signature the 'current' job context.
            No file I/O; just returns the path we will use for this job.
            """
            self._current_sig = signature
            # ensure parent dir still exists (in case base got wiped)
            self.base_dir.mkdir(parents=True, exist_ok=True)
            return str(self._path_for(signature))    
    
    def load(self, signature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        p = self._path_for(signature)
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return None
        except Exception:
            return None
        if data.get("version") != self.VERSION:
            return None
        if data.get("job") != signature:
            return None
        return data

    def save(self, signature: Dict[str, Any], state: Dict[str, Any]) -> None:
        """Atomic write: write tmp then replace."""
        p = self._path_for(signature)
        tmp = p.with_suffix(".json.tmp")
        payload = {
            "version": self.VERSION,
            "job": signature,
            **state,
            "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)

    def clear(self, signature: Dict[str, Any]) -> None:
        p = self._path_for(signature)
        try:
            p.unlink(missing_ok=True)
        except TypeError:
            if p.exists():
                try: p.unlink()
                except Exception: pass
        except Exception:
            pass
