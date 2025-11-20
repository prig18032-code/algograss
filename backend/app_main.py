# app_main.py
from fastapi import FastAPI

app = FastAPI(title="Algograss - DataShield API (Day 1)")

@app.get("/")
def root():
    return {"message": "Welcome to Algograss - API is running (Day 1)"}

@app.get("/health")
def health():
    return {"status": "ok"}
