from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from datasource_api import router as ds_router
from scanner_api import router as scan_router

app = FastAPI(title="Algograss - DataShield API", version="0.1.0")

# Allow all origins for now (local dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(ds_router)
app.include_router(scan_router)

# Static files (for UI)
BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def root():
    return {"status": "ok", "message": "Algograss backend running"}


@app.get("/ui")
def ui_root():
    """Main UI page"""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/ui/")
def ui_root_slash():
    """Handle /ui/ (with trailing slash) so it doesn't 404"""
    return FileResponse(STATIC_DIR / "index.html")
