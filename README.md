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
```

---

## Propuesta de evolución y escalabilidad

La solución está diseñada para el volumen actual del challenge, pero contempla una evolución natural si el tamaño o la frecuencia de las ingestas aumentan. En escenarios de mayor volumen, la capa de procesamiento podría migrar a Polars para mejorar rendimiento y uso de memoria. Asimismo, la ingesta batch podría desacoplarse del API y ejecutarse como procesos independientes.

A nivel de infraestructura, una evolución hacia la nube permitiría utilizar almacenamiento en objetos (por ejemplo, Cloud Storage) y servicios administrados para la ejecución de jobs y análisis analítico, manteniendo los mismos principios de Data Quality, trazabilidad y versionado de backups definidos en esta solución.