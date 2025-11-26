import json
import uuid
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException

router = APIRouter()

DATASOURCE_FILE = "datasources.json"


class DataSourceConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str


class DataSourceIn(BaseModel):
    name: str
    type: str
    config: DataSourceConfig


class DataSourceOut(DataSourceIn):
    id: str


def load_datasources():
    try:
        with open(DATASOURCE_FILE, "r") as f:
            return json.load(f)
    except:
        return []


def save_datasources(data):
    with open(DATASOURCE_FILE, "w") as f:
        json.dump(data, f, indent=4)


@router.get("/api/datasource/")
def list_datasources():
    return load_datasources()


@router.post("/api/datasource/")
def create_datasource(ds: DataSourceIn):

    data = load_datasources()

    record = ds.dict()
    record["id"] = str(uuid.uuid4())

    data.append(record)
    save_datasources(data)

    return {"message": "Datasource created successfully", "id": record["id"]}


@router.get("/api/datasource/{ds_id}")
def get_datasource(ds_id: str):

    data = load_datasources()

    for ds in data:
        if ds["id"] == ds_id:
            return ds

    raise HTTPException(404, "Datasource not found")
