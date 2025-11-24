from fastapi import APIRouter, HTTPException, Header
import psycopg2
import os
from scan_history import add_scan_result, list_history, get_history_entry

router = APIRouter()

API_KEY = os.environ.get("API_KEY", "changeme")


def require_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def load_datasources():
    import json
    if not os.path.exists("datasources.json"):
        return []
    with open("datasources.json") as f:
        return json.load(f)


@router.get("/{ds_id}")
def scan(ds_id: str, x_api_key: str = Header(None)):
    require_key(x_api_key)

    sources = load_datasources()
    ds = next((d for d in sources if d["id"] == ds_id), None)

    if not ds:
        raise HTTPException(status_code=404, detail="Datasource not found")

    cfg = ds["config"]

    try:
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {str(e)}")

    cur = conn.cursor()
    cur.execute("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog');
    """)

    rows = cur.fetchall()

    pii_keywords = ["email", "phone", "ip", "address", "token", "secret", "password"]

    schema = {}
    total_cols = 0
    pii_cols = 0

    for schema_name, table_name, col, dtype in rows:
        key = f"{schema_name}.{table_name}"
        if key not in schema:
            schema[key] = []

        is_pii = any(k in col.lower() for k in pii_keywords)
        if is_pii:
            pii_cols += 1

        total_cols += 1

        schema[key].append({
            "column": col,
            "type": dtype,
            "pii": is_pii
        })

    ratio = round((pii_cols / total_cols) * 100, 2) if total_cols else 0

    result = {
        "datasource_id": ds_id,
        "total_columns": total_cols,
        "pii_columns": pii_cols,
        "pii_ratio_percent": ratio,
        "schema": schema
    }

    entry_id = add_scan_result(ds_id, result)
    result["history_entry_id"] = entry_id

    return result


@router.get("/history/all")
def history(x_api_key: str = Header(None)):
    require_key(x_api_key)
    return list_history()


@router.get("/export/{entry_id}")
def export(entry_id: int, x_api_key: str = Header(None)):
    require_key(x_api_key)
    return get_history_entry(entry_id)
