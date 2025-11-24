from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.staticfiles import StaticFiles

from datasource_api import router as datasource_router
from scanner_api import router as scan_router

app = FastAPI(title="Algograss DataShield API", version="1.0")

# Mount UI
app.mount("/static", StaticFiles(directory="static"), name="static")

# Routers
app.include_router(datasource_router, prefix="/api/datasource", tags=["datasource"])
app.include_router(scan_router, prefix="/api/scan", tags=["scan"])


@app.get("/")
def root():
    return {"message": "Welcome to Algograss - API is running (UI at /ui)"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ui")
def ui():
    return {"ui": "/static/index.html"}


# ---- SWAGGER AUTHORIZE BUTTON SUPPORT BELOW ----

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Algograss DataShield API",
        version="1.0",
        description="GDPR / PII Scanner API",
        routes=app.routes,
    )

    # Add API Key Auth
    openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key"
        }
    }

    # Apply globally
    openapi_schema["security"] = [{"ApiKeyAuth": []}]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
