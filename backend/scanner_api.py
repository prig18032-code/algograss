# F:\algograss\backend\scanner_api.py
from fastapi import APIRouter, HTTPException
import os
import json
import psycopg2

from scan_history import add_scan_result, list_history

DB_FILE = "datasources.json"
router = APIRouter()


def read_db():
    if not os.path.exists(DB_FILE):
        return []
    with open(DB_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def classify_column_pii(column_name: str):
    """
    Very simple PII detection based on column name.
    This is real logic, using naming patterns only.
    """
    name = column_name.lower()
    reasons: list[str] = []

    if "email" in name:
        reasons.append("email")
    if "phone" in name or "mobile" in name or "contact" in name:
        reasons.append("phone")
    if "ip" in name:
        reasons.append("ip_address")
    if "name" in name and "user" in name:
        reasons.append("user_name")
    if "address" in name:
        reasons.append("postal_address")
    if "card" in name or "cc_" in name:
        reasons.append("card_number")
    if "ssn" in name or "nid" in name:
        reasons.append("national_id")

    return reasons


@router.get("/{ds_id}")
def scan_datasource(ds_id: str):
    """
    Scan a datasource by id:
    - Looks up the datasource in datasources.json
    - Connects to Postgres (Supabase) using psycopg2
    - Reads schema from information_schema.columns
    - Marks PII columns based on name
    - Saves scan result into scan_history.json
    """

    # 1) find datasource
    data = read_db()
    ds = None
    for d in data:
        if d.get("id") == ds_id:
            ds = d
            break

    if not ds:
        raise HTTPException(status_code=404, detail="Datasource not found")

    ds_type = ds.get("type", "").lower()
    config = ds.get("config", {})

    if ds_type not in ("postgres", "postgresql"):
        raise HTTPException(
            status_code=400,
            detail=f"Scanner only supports Postgres for now (got {ds_type})."
        )

    host = config.get("host")
    port = config.get("port", 5432)
    database = config.get("database")
    user = config.get("user")
    password = config.get("password")

    if not (host and database and user and password):
        raise HTTPException(
            status_code=400,
            detail="Missing connection info (host/database/user/password).",
        )

    # 2) connect to Postgres (Supabase Pooler) with SSL
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode="require",
            connect_timeout=5,
        )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Could not connect to Postgres: {str(e)}",
        )

    result: dict[str, list[dict]] = {}

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, ordinal_position;
            """
        )
        rows = cur.fetchall()

        for schema_name, table_name, column_name, data_type in rows:
            key = f"{schema_name}.{table_name}"
            pii_reasons = classify_column_pii(column_name)
            col_info = {
                "column": column_name,
                "type": data_type,
                "pii": len(pii_reasons) > 0,
                "pii_reason": pii_reasons,
            }
            result.setdefault(key, []).append(col_info)

        cur.close()
        conn.close()
    except Exception as e:
        conn.close()
        raise HTTPException(
            status_code=500,
            detail=f"Error scanning schema: {str(e)}",
        )

    # Build a summary
    total_cols = 0
    pii_cols = 0
    for cols in result.values():
        for col in cols:
            total_cols += 1
            if col.get("pii"):
                pii_cols += 1

    summary = {
        "total_columns": total_cols,
        "pii_columns": pii_cols,
        "pii_ratio": (pii_cols / total_cols) if total_cols else 0.0,
    }

    # Save scan in history
    entry = add_scan_result(ds_id, summary, result)

    return {
        "datasource_id": ds_id,
        "type": ds_type,
        "summary": summary,
        "schema": result,
        "history_entry_id": entry["id"],
    }


@router.get("/history")
def get_all_history():
    """
    Return all scan runs for all datasources.
    """
    return list_history(None)


@router.get("/history/{ds_id}")
def get_history_for_datasource(ds_id: str):
    """
    Return all scan runs for a specific datasource.
    """
    return list_history(ds_id)
