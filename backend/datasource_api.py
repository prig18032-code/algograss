# F:\algograss\backend\datasource_api.py
from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
import uuid
import json
import os

DB_FILE = "datasources.json"


def read_db():
    if not os.path.exists(DB_FILE):
        return []
    with open(DB_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def write_db(data):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


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


class DatasourceIn(BaseModel):
    name: str
    type: str  # e.g., "postgres"
    config: dict = {}


class DatasourceOut(DatasourceIn):
    id: str


router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.post("/", response_model=DatasourceOut)
def create_datasource(ds: DatasourceIn):
    data = read_db()
    ds_obj = ds.dict()
    ds_obj["id"] = str(uuid.uuid4())
    data.append(ds_obj)
    write_db(data)
    return ds_obj


@router.get("/", response_model=list[DatasourceOut])
def list_datasources():
    return read_db()


@router.get("/{ds_id}", response_model=DatasourceOut)
def get_datasource(ds_id: str):
    data = read_db()
    for d in data:
        if d["id"] == ds_id:
            return d
    raise HTTPException(status_code=404, detail="Datasource not found")
