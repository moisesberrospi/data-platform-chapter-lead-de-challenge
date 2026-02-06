from fastapi import FastAPI
from sqlalchemy import text
from pathlib import Path
from src.ingestion import ingest_all, ingest_departments, ingest_jobs, ingest_hired_employees
from pathlib import Path
from src.schemas import TransactionRequest
from src.transaction_service import process_transaction
from fastapi import HTTPException
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

@app.post("/ingest/departments")
def ingest_departments_endpoint():
    return ingest_departments(Path("/app/data/departments.csv"))

@app.post("/ingest/jobs")
def ingest_jobs_endpoint():
    return ingest_jobs(Path("/app/data/jobs.csv"))

@app.post("/ingest/hired-employees")
def ingest_hired_employees_endpoint():
    return ingest_hired_employees(Path("/app/data/hired_employees.csv"))

@app.post("/ingest/all")
def ingest_all_endpoint():
    return ingest_all()

@app.post("/transactions")
def transactions(req: TransactionRequest):
    result = process_transaction(table=req.table, rows=req.rows, mode=req.mode)

    # modo strict: si hay error, devolvemos 400 (transacción rechazada)
    if req.mode == "strict" and "error" in result:
        raise HTTPException(status_code=400, detail=result)

    return result