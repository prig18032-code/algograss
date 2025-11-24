from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
import json
import os
import uuid

router = APIRouter()

DATA_FILE = "datasources.json"
API_KEY = os.environ.get("API_KEY", "changeme")


class DataSourceIn(BaseModel):
    name: str
    type: str
    config: dict


def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def require_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


@router.get("/")
def list_datasources(x_api_key: str = Header(None)):
    require_key(x_api_key)
    return load_data()


@router.post("/")
def create_datasource(ds: DataSourceIn, x_api_key: str = Header(None)):
    require_key(x_api_key)
    data = load_data()

    new_ds = {
        "id": str(uuid.uuid4()),
        "name": ds.name,
        "type": ds.type,
        "config": ds.config
    }

    data.append(new_ds)
    save_data(data)
    return new_ds


@router.get("/{ds_id}")
def get_datasource(ds_id: str, x_api_key: str = Header(None)):
    require_key(x_api_key)
    data = load_data()
    for ds in data:
        if ds["id"] == ds_id:
            return ds
    raise HTTPException(status_code=404, detail="Datasource not found")
