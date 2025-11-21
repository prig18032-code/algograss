# F:\algograss\backend\app_main.py
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from datasource_api import router as ds_router
from scanner_api import router as scan_router

app = FastAPI(title="Algograss - DataShield API")

# API routers
app.include_router(ds_router, prefix="/api/datasource", tags=["datasource"])
app.include_router(scan_router, prefix="/api/scan", tags=["scan"])

# Serve static files (frontend UI)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return {"message": "Welcome to Algograss - API is running (UI at /ui)"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ui", response_class=FileResponse)
def ui():
    # Simple UI
    return FileResponse("static/index.html")
