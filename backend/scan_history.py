import json
import os
from datetime import datetime

FILE = "scan_history.json"


def load():
    if not os.path.exists(FILE):
        return []
    with open(FILE, "r") as f:
        return json.load(f)


def save(data):
    with open(FILE, "w") as f:
        json.dump(data, f, indent=2)


def add_scan_result(ds_id, result):
    data = load()
    entry = {
        "id": len(data) + 1,
        "datasource_id": ds_id,
        "timestamp": datetime.utcnow().isoformat(),
        "result": result
    }
    data.append(entry)
    save(data)
    return entry["id"]


def list_history():
    return load()


def get_history_entry(entry_id):
    data = load()
    for item in data:
        if item["id"] == entry_id:
            return item
    return None
