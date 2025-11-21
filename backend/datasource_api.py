# F:\algograss\backend\datasource_api.py
from fastapi import APIRouter, HTTPException
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

class DatasourceIn(BaseModel):
    name: str
    type: str  # e.g., "postgres"
    config: dict = {}

class DatasourceOut(DatasourceIn):
    id: str

router = APIRouter()

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
