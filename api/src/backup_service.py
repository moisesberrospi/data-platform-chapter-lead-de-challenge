import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text
from fastavro import writer, reader, parse_schema

from src.db import SessionLocal

BACKUP_ROOT = Path(os.getenv("BACKUP_ROOT", "/app/backups"))

SUPPORTED_TABLES = {"departments", "jobs", "hired_employees"}

def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _schema_for(table: str) -> Dict[str, Any]:
    # AVRO pragmático: tipos estables y portables
    if table == "departments":
        fields = [{"name": "id", "type": "int"}, {"name": "department", "type": "string"}]
    elif table == "jobs":
        fields = [{"name": "id", "type": "int"}, {"name": "job", "type": "string"}]
    elif table == "hired_employees":
        # datetime -> string ISO (simple y compatible)
        fields = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "datetime", "type": "string"},
            {"name": "department_id", "type": "int"},
            {"name": "job_id", "type": "int"},
        ]
    else:
        raise ValueError("tabla no soportada")

    return parse_schema({"type": "record", "name": f"{table}_record", "fields": fields})

def _fetch_rows(table: str) -> List[Dict[str, Any]]:
    with SessionLocal() as session:
        if table == "departments":
            rows = session.execute(text("SELECT id, department FROM departments ORDER BY id")).mappings().all()
            return [dict(r) for r in rows]

        if table == "jobs":
            rows = session.execute(text("SELECT id, job FROM jobs ORDER BY id")).mappings().all()
            return [dict(r) for r in rows]

        if table == "hired_employees":
            rows = session.execute(
                text("SELECT id, name, datetime, department_id, job_id FROM hired_employees ORDER BY id")
            ).mappings().all()
            out = []
            for r in rows:
                d = dict(r)
                d["datetime"] = d["datetime"].isoformat()
                out.append(d)
            return out

    raise ValueError("tabla no soportada")

def backup_table(table: str) -> Dict[str, Any]:
    if table not in SUPPORTED_TABLES:
        return {"status": "error", "error": "tabla no soportada", "supported": sorted(SUPPORTED_TABLES)}

    run_id = str(uuid.uuid4())
    stamp = _utc_stamp()
    version = f"{stamp}_{run_id}"

    out_dir = BACKUP_ROOT / table / version
    # Inmutable: si existe, debe fallar
    out_dir.mkdir(parents=True, exist_ok=False)

    schema = _schema_for(table)
    rows = _fetch_rows(table)

    avro_path = out_dir / "data.avro"
    meta_path = out_dir / "metadata.json"

    with avro_path.open("wb") as fo:
        writer(fo, schema, rows)

    metadata = {
        "table": table,
        "version": version,
        "run_id": run_id,
        "created_at_utc": stamp,
        "row_count": len(rows),
        "format": "avro",
        "files": {"data": "data.avro", "metadata": "metadata.json"},
    }
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    return {"status": "ok", "table": table, "version": version, "row_count": len(rows)}

def restore_table(table: str, version: str, mode: str = "truncate_insert") -> Dict[str, Any]:
    if table not in SUPPORTED_TABLES:
        return {"status": "error", "error": "tabla no soportada", "supported": sorted(SUPPORTED_TABLES)}

    in_dir = BACKUP_ROOT / table / version
    avro_path = in_dir / "data.avro"
    if not avro_path.exists():
        return {"status": "error", "error": "backup no encontrado", "table": table, "version": version}

    with avro_path.open("rb") as fo:
        records = list(reader(fo))

    with SessionLocal() as session:
        if mode == "truncate_insert":
            session.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))

        if table == "departments":
            if records:
                session.execute(text("INSERT INTO departments (id, department) VALUES (:id, :department)"), records)

        elif table == "jobs":
            if records:
                session.execute(text("INSERT INTO jobs (id, job) VALUES (:id, :job)"), records)

        elif table == "hired_employees":
            if records:
                session.execute(
                    text(
                        "INSERT INTO hired_employees (id, name, datetime, department_id, job_id) "
                        "VALUES (:id, :name, CAST(:datetime AS timestamp), :department_id, :job_id)"
                    ),
                    records,
                )

        session.commit()

    return {"status": "ok", "table": table, "version": version, "restored_rows": len(records)}
