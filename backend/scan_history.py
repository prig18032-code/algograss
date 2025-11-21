# F:\algograss\backend\scan_history.py
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

HISTORY_FILE = "scan_history.json"


def _read_history() -> List[Dict[str, Any]]:
    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def _write_history(data: List[Dict[str, Any]]) -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def add_scan_result(
    datasource_id: str,
    summary: Dict[str, Any],
    raw_schema: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Save a new scan result entry and return it.
    """
    history = _read_history()

    entry = {
        "id": len(history) + 1,  # simple incremental id
        "datasource_id": datasource_id,
        "scanned_at": datetime.utcnow().isoformat() + "Z",
        "summary": summary,
        "schema": raw_schema,
    }

    history.append(entry)
    _write_history(history)
    return entry


def list_history(datasource_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Return all history, or only entries for a specific datasource.
    """
    history = _read_history()
    if datasource_id:
        return [h for h in history if h.get("datasource_id") == datasource_id]
    return history


def get_history_entry(entry_id: int) -> Optional[Dict[str, Any]]:
    """
    Return a single history entry by its numeric id.
    """
    history = _read_history()
    for entry in history:
        if entry.get("id") == entry_id:
            return entry
    return None
