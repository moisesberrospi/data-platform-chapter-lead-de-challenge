# Data Platform – Chapter Lead Technical Challenge

Este proyecto implementa una plataforma de ingesta de datos con foco en **calidad, trazabilidad y reproducibilidad**, siguiendo buenas prácticas de data engineering y backend.

El objetivo es demostrar cómo abordar:
- Ingesta histórica (bulk)
- Validaciones de negocio
- Integridad referencial
- Observabilidad
- Backup & Recovery
- Métricas analíticas

---

## 🧱 Arquitectura

Componentes principales:

- **API (FastAPI)**: Ingesta, validación, backup y restore
- **PostgreSQL**: Persistencia relacional
- **Docker Compose**: Orquestación local
- **Filesystem**:
  - `data/` → archivos CSV
  - `backups/` → respaldos AVRO
  - `sql/` → métricas analíticas

---

## 🚀 Levantar el entorno

Requisitos:
- Docker
- Docker Compose
- PowerShell

```powershell
docker compose up -d --build
