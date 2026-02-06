import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import SessionLocal


def _stable_hash(row_data: Dict[str, Any]) -> str:
    # hash estable a partir del json (ordenado)
    payload = json.dumps(row_data, sort_keys=True, ensure_ascii=False)
    # sha256 en python (no dependemos de extensiones de postgres)
    import hashlib
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _reject(session: Session, run_id: str, source: str, table_name: str, reason: str, row_data: Dict[str, Any]) -> None:
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


def _parse_int(v: Any):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _parse_datetime(v: Any):
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


def _bump(reasons: Dict[str, int], key: str) -> None:
    reasons[key] = reasons.get(key, 0) + 1


def process_transaction(table: str, rows: List[Dict[str, Any]], mode: str = "strict") -> Dict[str, Any]:
    run_id = str(uuid.uuid4())

    reasons: Dict[str, int] = {}
    received = len(rows)
    inserted = 0
    rejected = 0

    source = "api_transaction"

    with SessionLocal() as session:
        valid_rows: List[Dict[str, Any]] = []
        rejects: List[Tuple[str, Dict[str, Any]]] = []

        if table in ("departments", "jobs"):
            # departments: id, department
            # jobs: id, job
            name_field = "department" if table == "departments" else "job"

            for r in rows:
                raw = dict(r)
                rid = _parse_int(raw.get("id"))
                name = (raw.get(name_field) or "").strip()

                if rid is None:
                    rejects.append(("invalid_id", raw)); _bump(reasons, "invalid_id"); continue
                if not name:
                    key = f"empty_{name_field}"
                    rejects.append((key, raw)); _bump(reasons, key); continue

                valid_rows.append({"id": rid, name_field: name})

            if mode == "strict" and rejects:
                return {
                    "run_id": run_id,
                    "table": table,
                    "mode": mode,
                    "received": received,
                    "inserted": 0,
                    "rejected": len(rejects),
                    "reasons": reasons,
                    "error": "transacción rechazada en modo strict (hay filas inválidas)",
                }

            # inserción (idempotente)
            sql = (
                f"INSERT INTO {table} (id, {name_field}) VALUES (:id, :name) "
                "ON CONFLICT (id) DO NOTHING"
            )
            payload = [{"id": vr["id"], "name": vr[name_field]} for vr in valid_rows]
            if payload:
                session.execute(text(sql), payload)
                inserted = len(payload)

            # registrar rechazos en partial
            if mode == "partial":
                for reason, raw in rejects:
                    _reject(session, run_id, source, table, reason, raw)
                rejected = len(rejects)

            session.commit()

            return {
                "run_id": run_id,
                "table": table,
                "mode": mode,
                "received": received,
                "inserted": inserted,
                "rejected": rejected,
                "reasons": reasons,
            }

        if table == "hired_employees":
            # hired_employees: id, name, datetime, department_id, job_id
            dept_ids_needed = set()
            job_ids_needed = set()

            # 1) validar formato y capturar ids requeridos
            parsed_rows = []
            for r in rows:
                raw = dict(r)
                emp_id = _parse_int(raw.get("id"))
                name = (raw.get("name") or "").strip()
                dt = _parse_datetime(raw.get("datetime"))
                dept_id = _parse_int(raw.get("department_id"))
                job_id = _parse_int(raw.get("job_id"))

                if emp_id is None:
                    rejects.append(("invalid_id", raw)); _bump(reasons, "invalid_id"); continue
                if not name:
                    rejects.append(("empty_name", raw)); _bump(reasons, "empty_name"); continue
                if dt is None:
                    rejects.append(("invalid_datetime", raw)); _bump(reasons, "invalid_datetime"); continue
                if dept_id is None:
                    rejects.append(("missing_department_id", raw)); _bump(reasons, "missing_department_id"); continue
                if job_id is None:
                    rejects.append(("missing_job_id", raw)); _bump(reasons, "missing_job_id"); continue

                dept_ids_needed.add(dept_id)
                job_ids_needed.add(job_id)
                parsed_rows.append((raw, emp_id, name, dt, dept_id, job_id))

            if mode == "strict" and rejects:
                return {
                    "run_id": run_id,
                    "table": table,
                    "mode": mode,
                    "received": received,
                    "inserted": 0,
                    "rejected": len(rejects),
                    "reasons": reasons,
                    "error": "transacción rechazada en modo strict (hay filas inválidas)",
                }

            # 2) validar integridad referencial (solo ids involucrados)
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
                    rejects.append(("department_fk_not_found", raw)); _bump(reasons, "department_fk_not_found"); continue
                if job_id not in existing_jobs:
                    rejects.append(("job_fk_not_found", raw)); _bump(reasons, "job_fk_not_found"); continue

                valid_rows.append(
                    {
                        "id": emp_id,
                        "name": name,
                        "datetime": dt,
                        "department_id": dept_id,
                        "job_id": job_id,
                    }
                )

            if mode == "strict" and rejects:
                return {
                    "run_id": run_id,
                    "table": table,
                    "mode": mode,
                    "received": received,
                    "inserted": 0,
                    "rejected": len(rejects),
                    "reasons": reasons,
                    "error": "transacción rechazada en modo strict (hay filas inválidas o FK no cumple)",
                }

            # 3) insertar válidos
            if valid_rows:
                session.execute(
                    text(
                        "INSERT INTO hired_employees (id, name, datetime, department_id, job_id) "
                        "VALUES (:id, :name, :datetime, :department_id, :job_id) "
                        "ON CONFLICT (id) DO NOTHING"
                    ),
                    valid_rows,
                )
                inserted = len(valid_rows)

            # 4) registrar rechazos (solo partial)
            if mode == "partial":
                for reason, raw in rejects:
                    _reject(session, run_id, source, table, reason, raw)
                rejected = len(rejects)

            session.commit()

            return {
                "run_id": run_id,
                "table": table,
                "mode": mode,
                "received": received,
                "inserted": inserted,
                "rejected": rejected,
                "reasons": reasons,
            }

        # tabla inválida
        return {
            "run_id": run_id,
            "table": table,
            "mode": mode,
            "received": received,
            "inserted": 0,
            "rejected": received,
            "reasons": {"invalid_table": received},
            "error": "tabla no soportada",
        }
