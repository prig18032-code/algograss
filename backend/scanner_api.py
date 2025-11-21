# F:\algograss\backend\scanner_api.py
from fastapi import APIRouter, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
import os
import json
import psycopg2

from scan_history import add_scan_result, list_history, get_history_entry

DB_FILE = "datasources.json"


def read_db():
    if not os.path.exists(DB_FILE):
        return []
    with open(DB_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def verify_api_key(x_api_key: str | None = Header(default=None)):
    """
    Optional API key auth.
    If environment variable ALGOGRASS_API_KEY is set,
    all requests must include header: X-API-Key: <that value>.
    If the env var is NOT set, auth is effectively disabled.
    """
    expected = os.getenv("ALGOGRASS_API_KEY")
    if not expected:
        # Auth disabled
        return
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


router = APIRouter(dependencies=[Depends(verify_api_key)])


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


def compute_table_risk(table_cols: list[dict]) -> dict:
    """
    Compute risk stats for a single table.
    Returns dict with total_columns, pii_columns, pii_ratio, risk_level.
    """

    total_cols = len(table_cols)
    pii_cols = sum(1 for c in table_cols if c.get("pii"))

    if total_cols == 0:
        pii_ratio = 0.0
    else:
        pii_ratio = pii_cols / total_cols

    # Check for "high sensitivity" PII indicators
    high_sensitivity_types = {"email", "phone", "card_number", "national_id"}
    high_sensitivity_present = False

    for col in table_cols:
        if not col.get("pii"):
            continue
        reasons = set(col.get("pii_reason") or [])
        if reasons & high_sensitivity_types:
            high_sensitivity_present = True
            break

    # Basic risk rules
    # (you can tweak these later as product “policy”)
    if pii_cols == 0:
        risk_level = "none"
    elif pii_ratio > 0.30 or (high_sensitivity_present and pii_cols >= 3):
        risk_level = "high"
    elif pii_ratio > 0.10:
        risk_level = "medium"
    else:
        risk_level = "low"

    return {
        "total_columns": total_cols,
        "pii_columns": pii_cols,
        "pii_ratio": pii_ratio,
        "risk_level": risk_level,
    }


def compute_overall_risk(table_risks: dict) -> str:
    """
    Aggregate per-table risk into a single overall risk level.
    Priority: high > medium > low > none.
    """
    levels = [info.get("risk_level", "none") for info in table_risks.values()]

    if any(l == "high" for l in levels):
        return "high"
    if any(l == "medium" for l in levels):
        return "medium"
    if any(l == "low" for l in levels):
        return "low"
    return "none"


@router.get("/{ds_id}")
def scan_datasource(ds_id: str):
    """
    Scan a datasource by id:
    - Looks up the datasource in datasources.json
    - Connects to Postgres (Supabase) using psycopg2
    - Reads schema from information_schema.columns
    - Marks PII columns based on name
    - Computes table-level & overall risk
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

    # Build global summary
    total_cols = 0
    pii_cols = 0
    for cols in result.values():
        for col in cols:
            total_cols += 1
            if col.get("pii"):
                pii_cols += 1

    pii_ratio = (pii_cols / total_cols) if total_cols else 0.0

    # Per-table risk
    table_risks: dict[str, dict] = {}
    for table_name, cols in result.items():
        table_risks[table_name] = compute_table_risk(cols)

    overall_risk = compute_overall_risk(table_risks)

    summary = {
        "total_columns": total_cols,
        "pii_columns": pii_cols,
        "pii_ratio": pii_ratio,
        "overall_risk": overall_risk,
        "table_risks": table_risks,
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


@router.get("/export/{entry_id}")
def export_scan(entry_id: int):
    """
    Download a JSON file for a specific scan history entry.
    """
    entry = get_history_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Scan history entry not found")

    payload = {
        "datasource_id": entry["datasource_id"],
        "scanned_at": entry["scanned_at"],
        "summary": entry.get("summary", {}),
        "schema": entry.get("schema", {}),
    }

    return JSONResponse(
        content=payload,
        headers={
            "Content-Disposition": f'attachment; filename="scan_{entry_id}.json"'
        },
    )
