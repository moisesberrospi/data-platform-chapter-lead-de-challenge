import csv
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import SessionLocal

# CSV montados en el contenedor (docker-compose: ./data:/app/data)
DATA_DIR = Path("/app/data")


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    v = str(value).strip()
    if v == "":
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    v = str(value).strip()
    if v == "":
        return None
    try:
        # soporta "2021-01-01T00:00:00Z"
        if v.endswith("Z"):
            v = v[:-1]
        return datetime.fromisoformat(v)
    except ValueError:
        return None


def _stable_hash(row_data: Dict[str, Any]) -> str:
    payload = json.dumps(row_data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _reject(
    session: Session,
    run_id: str,
    source: str,
    table_name: str,
    reason: str,
    row_data: Dict[str, Any],
) -> None:
    row_hash = _stable_hash(row_data)
    row_data_json = json.dumps(row_data, ensure_ascii=False)

    session.execute(
        text(
            "INSERT INTO dq_rejections (run_id, row_hash, source, table_name, reason, row_data) "
            "VALUES (:run_id, :row_hash, :source, :table_name, :reason, CAST(:row_data AS json)) "
            "ON CONFLICT ON CONSTRAINT uq_dq_rejections_run_hash_reason DO NOTHING"
        ),
        {
            "run_id": run_id,
            "row_hash": row_hash,
            "source": source,
            "table_name": table_name,
            "reason": reason,
            "row_data": row_data_json,
        },
    )




def ingest_departments(csv_path: Path, run_id: Optional[str] = None) -> Dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())

    inserted_attempted = 0
    rejected = 0
    reasons: Dict[str, int] = {}

    def bump(reason: str):
        nonlocal rejected
        rejected += 1
        reasons[reason] = reasons.get(reason, 0) + 1

    with SessionLocal() as session:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                raw = {
                    "id": row[0] if len(row) > 0 else None,
                    "department": row[1] if len(row) > 1 else None,
                }

                dept_id = _parse_int(raw["id"])
                dept_name = (raw["department"] or "").strip()

                if dept_id is None:
                    bump("invalid_id")
                    _reject(session, run_id, csv_path.name, "departments", "invalid_id", raw)
                    continue

                if not dept_name:
                    bump("empty_department")
                    _reject(session, run_id, csv_path.name, "departments", "empty_department", raw)
                    continue

                session.execute(
                    text(
                        "INSERT INTO departments (id, department) VALUES (:id, :department) "
                        "ON CONFLICT (id) DO NOTHING"
                    ),
                    {"id": dept_id, "department": dept_name},
                )
                inserted_attempted += 1

        session.commit()

    return {
        "run_id": run_id,
        "source": csv_path.name,
        "table": "departments",
        "inserted_attempted": inserted_attempted,
        "rejected": rejected,
        "reasons": reasons,
    }


def ingest_jobs(csv_path: Path, run_id: Optional[str] = None) -> Dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())

    inserted_attempted = 0
    rejected = 0
    reasons: Dict[str, int] = {}

    def bump(reason: str):
        nonlocal rejected
        rejected += 1
        reasons[reason] = reasons.get(reason, 0) + 1

    with SessionLocal() as session:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                raw = {
                    "id": row[0] if len(row) > 0 else None,
                    "job": row[1] if len(row) > 1 else None,
                }

                job_id = _parse_int(raw["id"])
                job_name = (raw["job"] or "").strip()

                if job_id is None:
                    bump("invalid_id")
                    _reject(session, run_id, csv_path.name, "jobs", "invalid_id", raw)
                    continue

                if not job_name:
                    bump("empty_job")
                    _reject(session, run_id, csv_path.name, "jobs", "empty_job", raw)
                    continue

                session.execute(
                    text(
                        "INSERT INTO jobs (id, job) VALUES (:id, :job) "
                        "ON CONFLICT (id) DO NOTHING"
                    ),
                    {"id": job_id, "job": job_name},
                )
                inserted_attempted += 1

        session.commit()

    return {
        "run_id": run_id,
        "source": csv_path.name,
        "table": "jobs",
        "inserted_attempted": inserted_attempted,
        "rejected": rejected,
        "reasons": reasons,
    }


def ingest_hired_employees(csv_path: Path, run_id: Optional[str] = None) -> Dict[str, Any]:
    run_id = run_id or str(uuid.uuid4())

    inserted_attempted = 0
    rejected = 0
    reasons: Dict[str, int] = {}

    def bump(reason: str):
        nonlocal rejected
        rejected += 1
        reasons[reason] = reasons.get(reason, 0) + 1

    with SessionLocal() as session:
        # Para este tamaño está OK cargar en memoria.
        # Para 10GB: validar FK vía JOIN/SQL set-based o staging + constraints.
        dept_ids = {r[0] for r in session.execute(text("SELECT id FROM departments")).fetchall()}
        job_ids = {r[0] for r in session.execute(text("SELECT id FROM jobs")).fetchall()}

        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                raw = {
                    "id": row[0] if len(row) > 0 else None,
                    "name": row[1] if len(row) > 1 else None,
                    "datetime": row[2] if len(row) > 2 else None,
                    "department_id": row[3] if len(row) > 3 else None,
                    "job_id": row[4] if len(row) > 4 else None,
                }

                emp_id = _parse_int(raw["id"])
                name = (raw["name"] or "").strip()
                dt = _parse_datetime(raw["datetime"])
                department_id = _parse_int(raw["department_id"])
                job_id = _parse_int(raw["job_id"])

                if emp_id is None:
                    bump("invalid_id")
                    _reject(session, run_id, csv_path.name, "hired_employees", "invalid_id", raw)
                    continue
                if not name:
                    bump("empty_name")
                    _reject(session, run_id, csv_path.name, "hired_employees", "empty_name", raw)
                    continue
                if dt is None:
                    bump("invalid_datetime")
                    _reject(session, run_id, csv_path.name, "hired_employees", "invalid_datetime", raw)
                    continue

                if department_id is not None and department_id not in dept_ids:
                    bump("department_fk_not_found")
                    _reject(session, run_id, csv_path.name, "hired_employees", "department_fk_not_found", raw)
                    continue

                if job_id is not None and job_id not in job_ids:
                    bump("job_fk_not_found")
                    _reject(session, run_id, csv_path.name, "hired_employees", "job_fk_not_found", raw)
                    continue

                session.execute(
                    text(
                        "INSERT INTO hired_employees (id, name, datetime, department_id, job_id) "
                        "VALUES (:id, :name, :datetime, :department_id, :job_id) "
                        "ON CONFLICT (id) DO NOTHING"
                    ),
                    {
                        "id": emp_id,
                        "name": name,
                        "datetime": dt,
                        "department_id": department_id,
                        "job_id": job_id,
                    },
                )
                inserted_attempted += 1

        session.commit()

    return {
        "run_id": run_id,
        "source": csv_path.name,
        "table": "hired_employees",
        "inserted_attempted": inserted_attempted,
        "rejected": rejected,
        "reasons": reasons,
    }


def ingest_all(data_dir: Path = DATA_DIR) -> Dict[str, Any]:
    """
    Ejecuta toda la ingesta con un run_id único para trazabilidad.
    """
    run_id = str(uuid.uuid4())

    results = []
    results.append(ingest_departments(data_dir / "departments.csv", run_id=run_id))
    results.append(ingest_jobs(data_dir / "jobs.csv", run_id=run_id))
    results.append(ingest_hired_employees(data_dir / "hired_employees.csv", run_id=run_id))

    return {"run_id": run_id, "results": results}
