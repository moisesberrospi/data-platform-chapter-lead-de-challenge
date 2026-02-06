# 📊 Data Platform – Chapter Lead Technical Challenge

Plataforma de ingesta de datos con foco en **calidad, trazabilidad y reproducibilidad**, siguiendo buenas prácticas de data engineering y backend.

## 🎯 Objetivos

- ✅ Ingesta histórica (bulk)
- ✅ Validaciones de negocio
- ✅ Integridad referencial
- ✅ Observabilidad y trazabilidad
- ✅ Backup & Recovery
- ✅ Métricas analíticas

---

## 🏗️ Arquitectura

| Componente | Rol |
|-----------|-----|
| 🌐 **FastAPI** | API REST: ingesta, validación, backup/restore |
| 🗄️ **PostgreSQL** | Persistencia relacional con integridad referencial |
| 🐳 **Docker Compose** | Orquestación del entorno local |
| 📁 **Filesystem** | `data/` (CSV), `backups/` (AVRO), `sql/` (métricas) |

---
---

## 🚀 Demo End-to-End

> ℹ️ Todos los comandos están optimizados para **PowerShell**

### 1️⃣ Levantar el entorno

```powershell
docker compose up -d --build
```

### 2️⃣ Verificar salud del servicio

```powershell
curl.exe http://localhost:8081/health
```

**Respuesta esperada:**
```json
{ "status": "ok" }
```

### 3️⃣ Reset de datos (opcional, para demo limpia)

```powershell
docker compose exec db psql -U challenge -d challenge -c `
"TRUNCATE hired_employees, departments, jobs, dq_rejections RESTART IDENTITY CASCADE;"
```

### 4️⃣ Ejecutar ingesta histórica (Bulk Migration)

```powershell
curl.exe -X POST http://localhost:8081/ingest/all
```

**Devuelve:**
- `run_id` de la ejecución
- Registros insertados y rechazados por tabla

### 5️⃣ Validar datos cargados

```powershell
# Departments
docker compose exec db psql -U challenge -d challenge -c "SELECT COUNT(*) FROM departments;"

# Jobs
docker compose exec db psql -U challenge -d challenge -c "SELECT COUNT(*) FROM jobs;"

# Employed
docker compose exec db psql -U challenge -d challenge -c "SELECT COUNT(*) FROM hired_employees;"
```

### 6️⃣ Revisar Data Quality (rechazos)

```powershell
docker compose exec db psql -U challenge -d challenge -c `
"SELECT reason, COUNT(*) FROM dq_rejections GROUP BY reason;"
```

### 7️⃣ Probar API de transacciones (1–1000 registros)

```powershell
$body = @{
  table = "departments"
  mode  = "strict"
  rows  = @(
    @{ id = 999; department = "Demo Dept" }
  )
}

Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8081/transactions" `
  -ContentType "application/json" `
  -Body ($body | ConvertTo-Json -Depth 5)
```

### 8️⃣ Probar validación de integridad referencial ⚠️

```powershell
$body = @{
  table = "hired_employees"
  mode  = "strict"
  rows  = @(
    @{
      id = 9999
      name = "Empleado Test"
      datetime = "2021-02-01T10:00:00"
      department_id = 999
      job_id = 999
    }
  )
}

Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8081/transactions" `
  -ContentType "application/json" `
  -Body ($body | ConvertTo-Json -Depth 5)
```

> ❌ Esperado: error de integridad referencial

### 9️⃣ Ejecutar métricas SQL (Analytics)

```powershell
docker compose exec db psql -U challenge -d challenge -f /sql/metrics.sql
```

**Incluye:**
- 📊 **Métrica A:** contrataciones por departamento, cargo y trimestre (2021)
- 📈 **Métrica B:** departamentos por encima del promedio de contratación

### 🔟 Generar backup (AVRO)

```powershell
docker compose exec api python src/backup_service.py --table hired_employees
```

**Ver backups:**
```powershell
docker compose exec api ls -la /app/backups
```

### 1️⃣1️⃣ Restaurar desde backup

```powershell
docker compose exec api python src/backup_service.py `
  --restore `
  --table hired_employees `
  --backup_id <backup_id>
```

### 1️⃣2️⃣ Revisar logs del servicio

```powershell
docker compose logs --tail=100 api
```

---

## ✅ Lo que se demuestra

| Aspecto | Status |
|--------|--------|
| Ingesta histórica | ✓ |
| Data Quality | ✓ |
| API transaccional | ✓ |
| Integridad referencial | ✓ |
| Métricas SQL | ✓ |
| Backup & Recovery | ✓ |
| Observabilidad | ✓ |
---

## 🚀 Roadmap: Evolución y Escalabilidad

La solución actual cubre el alcance del challenge, pero contempla una **evolución natural** para mayores volúmenes.

### 📈 Si el volumen aumenta...

#### **Procesamiento: Pandas → Polars**
- 🔀 Ejecución paralela nativa
- 💾 Menor consumo de memoria (Apache Arrow)
- 🔧 Transparente para la lógica de negocio

#### **Desacoplamiento: API ↔ Ingesta Batch**
```
Arquitectura Actual:        API + Ingesta Batch (acoplado)
                ↓
Arquitectura Escalable:     API (transacciones) + Jobs async (ingesta pesada)
```

**Ventajas:**
- Paralelización
- Reintentos controlados
- Independencia de ciclos

#### **Infraestructura: On-Prem → Cloud-Native (GCP)**
- ☁️ **Storage:** Cloud Storage (inmutable, versionado)
- 🏃 **Computación:** Cloud Functions / Cloud Run
- 📊 **Analytics:** BigQuery con proyecciones
- 🔍 **Data Quality:** Mantiene trazabilidad y `run_id`

**Invariantes:** La Data Quality, versionado y auditoría se mantienen intactos.