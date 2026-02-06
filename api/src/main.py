from fastapi import FastAPI
from sqlalchemy import text

from src.db import engine

app = FastAPI(title="Reto Chapter Lead Data Engineer")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/db")
def health_db():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok", "db": "reachable"}
