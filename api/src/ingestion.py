import csv
import json
import os
import uuid
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import SessionLocal


# =========================
# Config
# =========================
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

T = TypeVar("T")


def chunked(items: List[T], size: int) -> Iterable[List[T]]:
    """Divide una lista en lotes para evitar inserts gigantes (locks/memoria)."""
    if size <= 0:
        yield items
        return
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _open_csv_dictreader(csv_path: Path, expected_headers: List[str]) -> csv.DictReader:
    """
    Soporta CSV con o sin headers.
    - utf-8-sig elimina BOM
    - Sniffer detecta delimitador
    - Si la primera fila NO parece header, asigna expected_headers y trata la primera fila como data.
    """
    f = csv_path.open("r", encoding="utf-8-sig", newline="")
    sample = f.read(4096)
    f.seek(0)

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
    except csv.Error:
        dialect = csv.excel

    # Lee primera fila para detectar si hay header
    pos = f.tell()
    first_line = f.readline()
    f.seek(pos)

    # Parse de primera fila para comparar con expected headers
    first_row = next(csv.reader([first_line], dialect=dialect), [])
    first_row_norm = [c.strip().lower() for c in first_row]
    expected_norm = [c.strip().lower() for c in expected_headers]

    has_header = first_row_norm == expected_norm

    if has_header:
        return csv.DictReader(f, dialect=dialect)

    # No tiene header: DictReader con fieldnames fijos + saltar “header” manualmente
    reader = csv.DictReader(f, fieldnames=expected_headers, dialect=dialect)
    return reader



# =========================
# Helpers DQ
# =========================
def _bump(reasons: Dict[str, int], key: str) -> None:
    reasons[key] = reasons.get(key, 0) + 1


def _parse_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _parse_datetime(v: Any) -> Optional[datetime]:
    """Parse ISO-8601 con tolerancia a sufijo Z."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _stable_hash(row_data: Dict[str, Any]) -> str:
    """Hash estable (sha256) sobre JSON ordenado para deduplicación por corrida."""
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
    """
    Registra rechazo DQ.
    - row_data se envía como string JSON para evitar "can't adapt type 'dict'"
    - CAST(:row_data AS json) evita problemas con :row_data::json en algunos parsers
    - ON CONFLICT evita duplicados por (run_id,row_hash,reason)
    """
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


# =========================
# Ingest CSV -> DB
# =========================
def ingest_departments(csv_path: Path, run_id: str) -> Dict[str, Any]:
    reasons: Dict[str, int] = {}
    inserted = 0
    rejected = 0
    payload: List[Dict[str, Any]] = []
    source = csv_path.name

    reader = _open_csv_dictreader(csv_path, expected_headers=["id", "department"])
    f = reader._fieldnames  # type: ignore[attr-defined]
    if not f or "id" not in (f or []) or "department" not in (f or []):
        # headers inesperados => rechazar todo con motivo claro
        return {
            "source": source,
            "table": "departments",
            "inserted": 0,
            "rejected": 0,
            "reasons": {"invalid_headers": 1, "headers_seen": 1},
            "headers_seen": f,
        }

    for row in reader:
        raw = dict(row)
        rid = _parse_int(raw.get("id"))
        department = (raw.get("department") or "").strip()

        if rid is None:
            rejected += 1
            _bump(reasons, "invalid_id")
            continue
        if not department:
            rejected += 1
            _bump(reasons, "empty_department")
            continue

        payload.append({"id": rid, "department": department})

    # cerrar file handle interno del reader
    try:
        reader.reader.f.close()  # type: ignore
    except Exception:
        pass

    with SessionLocal() as session:
        if payload:
            sql = text(
                "INSERT INTO departments (id, department) "
                "VALUES (:id, :department) "
                "ON CONFLICT (id) DO NOTHING"
            )
            for batch in chunked(payload, BATCH_SIZE):
                session.execute(sql, batch)
                inserted += len(batch)

        session.commit()

    return {
        "source": source,
        "table": "departments",
        "inserted": inserted,
        "rejected": rejected,
        "reasons": reasons,
    }


def ingest_jobs(csv_path: Path, run_id: str) -> Dict[str, Any]:
    reasons: Dict[str, int] = {}
    inserted = 0
    rejected = 0
    payload: List[Dict[str, Any]] = []
    source = csv_path.name

    reader = _open_csv_dictreader(csv_path, expected_headers=["id", "job"])
    f = reader._fieldnames  # type: ignore[attr-defined]
    if not f or "id" not in (f or []) or "job" not in (f or []):
        return {
            "source": source,
            "table": "jobs",
            "inserted": 0,
            "rejected": 0,
            "reasons": {"invalid_headers": 1, "headers_seen": 1},
            "headers_seen": f,
        }

    for row in reader:
        raw = dict(row)
        rid = _parse_int(raw.get("id"))
        job = (raw.get("job") or "").strip()

        if rid is None:
            rejected += 1
            _bump(reasons, "invalid_id")
            continue
        if not job:
            rejected += 1
            _bump(reasons, "empty_job")
            continue

        payload.append({"id": rid, "job": job})

    try:
        reader.reader.f.close()  # type: ignore
    except Exception:
        pass

    with SessionLocal() as session:
        if payload:
            sql = text(
                "INSERT INTO jobs (id, job) "
                "VALUES (:id, :job) "
                "ON CONFLICT (id) DO NOTHING"
            )
            for batch in chunked(payload, BATCH_SIZE):
                session.execute(sql, batch)
                inserted += len(batch)

        session.commit()

    return {
        "source": source,
        "table": "jobs",
        "inserted": inserted,
        "rejected": rejected,
        "reasons": reasons,
    }


def ingest_hired_employees(csv_path: Path, run_id: str) -> Dict[str, Any]:
    reasons: Dict[str, int] = {}
    inserted = 0
    rejected = 0

    source = csv_path.name
    table_name = "hired_employees"

    valid_rows: List[Dict[str, Any]] = []
    rejects: List[Tuple[str, Dict[str, Any]]] = []

    dept_ids_needed = set()
    job_ids_needed = set()
    parsed_rows: List[Tuple[Dict[str, Any], int, str, datetime, int, int]] = []

    reader = _open_csv_dictreader(csv_path, expected_headers=["id", "name", "datetime", "department_id", "job_id"])
    f = reader._fieldnames  # type: ignore[attr-defined]
    expected = {"id", "name", "datetime", "department_id", "job_id"}
    if not f or not expected.issubset(set(f or [])):
        return {
            "source": source,
            "table": "hired_employees",
            "inserted": 0,
            "rejected": 0,
            "reasons": {"invalid_headers": 1, "headers_seen": 1},
            "headers_seen": f,
        }

    # 1) Validación de tipos + obligatoriedad (DD)
    for row in reader:
        raw = dict(row)

        emp_id = _parse_int(raw.get("id"))
        name = (raw.get("name") or "").strip()
        dt = _parse_datetime(raw.get("datetime"))
        dept_id = _parse_int(raw.get("department_id"))
        job_id = _parse_int(raw.get("job_id"))

        if emp_id is None:
            rejects.append(("invalid_id", raw))
            _bump(reasons, "invalid_id")
            continue
        if not name:
            rejects.append(("empty_name", raw))
            _bump(reasons, "empty_name")
            continue
        if dt is None:
            rejects.append(("invalid_datetime", raw))
            _bump(reasons, "invalid_datetime")
            continue
        if dept_id is None:
            rejects.append(("missing_department_id", raw))
            _bump(reasons, "missing_department_id")
            continue
        if job_id is None:
            rejects.append(("missing_job_id", raw))
            _bump(reasons, "missing_job_id")
            continue

        dept_ids_needed.add(dept_id)
        job_ids_needed.add(job_id)
        parsed_rows.append((raw, emp_id, name, dt, dept_id, job_id))

    try:
        reader.reader.f.close()  # type: ignore
    except Exception:
        pass

    # 2) Integridad referencial (solo IDs involucrados)
    with SessionLocal() as session:
        existing_depts = set()
        existing_jobs = set()

        if dept_ids_needed:
            rows_dept = session.execute(
                text("SELECT id FROM departments WHERE id = ANY(:ids)"),
                {"ids": list(dept_ids_needed)},
            ).fetchall()
            existing_depts = {x[0] for x in rows_dept}

        if job_ids_needed:
            rows_job = session.execute(
                text("SELECT id FROM jobs WHERE id = ANY(:ids)"),
                {"ids": list(job_ids_needed)},
            ).fetchall()
            existing_jobs = {x[0] for x in rows_job}

        for raw, emp_id, name, dt, dept_id, job_id in parsed_rows:
            if dept_id not in existing_depts:
                rejects.append(("department_fk_not_found", raw))
                _bump(reasons, "department_fk_not_found")
                continue
            if job_id not in existing_jobs:
                rejects.append(("job_fk_not_found", raw))
                _bump(reasons, "job_fk_not_found")
                continue

            valid_rows.append(
                {
                    "id": emp_id,
                    "name": name,
                    "datetime": dt,
                    "department_id": dept_id,
                    "job_id": job_id,
                }
            )

        # 3) Insert por lotes (Batch Loading)
        if valid_rows:
            sql = text(
                "INSERT INTO hired_employees (id, name, datetime, department_id, job_id) "
                "VALUES (:id, :name, :datetime, :department_id, :job_id) "
                "ON CONFLICT (id) DO NOTHING"
            )
            for batch in chunked(valid_rows, BATCH_SIZE):
                session.execute(sql, batch)
                inserted += len(batch)

        # 4) Registrar rechazos DQ
        for reason, raw in rejects:
            _reject(session, run_id, source, table_name, reason, raw)

        rejected = len(rejects)
        session.commit()

    return {
        "source": source,
        "table": "hired_employees",
        "inserted": inserted,
        "rejected": rejected,
        "reasons": reasons,
    }


def ingest_all(data_dir: Path = DATA_DIR) -> Dict[str, Any]:
    """
    Ingesta histórica desde CSV:
    - departments.csv
    - jobs.csv
    - hired_employees.csv
    """
    run_id = str(uuid.uuid4())

    results = []
    results.append(ingest_departments(data_dir / "departments.csv", run_id=run_id))
    results.append(ingest_jobs(data_dir / "jobs.csv", run_id=run_id))
    results.append(ingest_hired_employees(data_dir / "hired_employees.csv", run_id=run_id))

    return {"run_id": run_id, "results": results}
