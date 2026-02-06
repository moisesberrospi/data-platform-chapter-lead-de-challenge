# 📋 DECISIONS.md – Architectural Decision Records

Resumen de decisiones técnicas y el razonamiento detrás de cada una.

---


### Pilares de la solución:

#### 🔄 **Ingesta Histórica**
- Proceso batch para cargar CSV (departments, jobs, hired_employees) hacia PostgreSQL
- Carga por **lotes** en lugar de fila a fila → evita locks y mejora throughput
- Capa explícita de **Data Quality**: registros inválidos se persisten en tabla de rechazos
- Trazabilidad por `run_id`, motivo del error y dato original

#### 🌐 **API REST**
- Endpoint único que recibe 1 a 1000 registros para cualquier tabla
- Validaciones centralizadas en el servicio: tipos de datos, campos obligatorios, integridad referencial
- Para `hired_employees`: validación de IDs de departamento y cargo antes de insertar
- Evita empleados huérfanos y reglas de negocio distribuidas

#### 🗄️ **Gestión de Esquema**
- **Alembic** para versionado de base de datos
- Evolución del modelo sin resets manuales
- Principio: el esquema es código y debe versionarse

#### 💾 **Backup & Recovery**
- Formato **AVRO**: binario eficiente con esquema incluido
- Backups: inmutables, versionados y con checksum
- Restauraciones confiables y auditables

#### 📊 **Análisis de Datos**
- Métricas en SQL optimizado, separadas de la API
- Ejecutables sin dependencia del backend

#### 🐳 **Infraestructura**
- Docker Compose para reproducibilidad
- Healthchecks, logs y trazabilidad por ejecución
- Observabilidad: visibilidad en fallos, motivos y datos rechazados

---

## 1️⃣ Uso de FastAPI para la API REST

| | |
|------|------|
| **Decisión** | FastAPI como framework web |
| **Ventajas** | Alto rendimiento, tipado explícito, fácil integración con Pydantic |
| **Alternativas** | Flask, Django REST |

**Por qué FastAPI:** Framework liviano, auto-documentado y con validación automática de tipos.

---

## 2️⃣ PostgreSQL como base de datos

| | |
|------|------|
| **Decisión** | PostgreSQL como motor relacional |
| **Ventajas** | Integridad referencial, JSON support, funciones analíticas |
| **Estándar** | Referencia en entornos productivos |

---

## 3️⃣ Data Quality separada de la data válida

### ❌ Sin esta decisión:
- Registros inválidos se descartan
- Pérdida de información
- Sin posibilidad de auditoría

### ✅ Con esta decisión:
- **Tabla `dq_rejections`** → registro por fila rechazada
- **Asociación mediante `run_id`** → trazabilidad completa
- **Motivo del error** → diagnóstico rápido

**Beneficios:**
- 🔍 Auditoría de calidad
- 🔄 Reprocesamiento
- 📊 Observabilidad real del pipeline

---

## 4️⃣ Batch Loading para ingesta histórica

| aspecto | detalle |
|--------|---------|
| **Patrón** | Carga por lotes, no fila a fila |
| **Beneficio 1** | Evita locks prolongados |
| **Beneficio 2** | Reduce consumo de memoria |
| **Escalabilidad** | Preparado para volúmenes mayores |

---

## 5️⃣ AVRO para Backup & Recovery

### Características:
- ✅ **Binario eficiente** → tamaño reducido
- ✅ **Esquema incluido** → auto-documentado
- ✅ **Inmutable** → sin cambios posteriores
- ✅ **Versionado** → trazabilidad de versiones

No es un dump, es un **artefacto de plataforma** restaurable y auditable.

---

## 6️⃣ SQL separado para métricas

### Ventajas:
| | |
|---|---|
| 🔀 | Separación de responsabilidades |
| 👥 | Facilitá revisión por analistas |
| 📝 | Reproducible y auditable |

Las queries analíticas son independientes del API → cualquiera puede ejecutarlas.

---

## 7️⃣ Docker y Docker Compose

### Justificación:
```
Reproducibilidad   ✓
Onboarding rápido  ✓
DevOps moderno     ✓
```

---

## 8️⃣ Propuesta de mejora y escalabilidad

### 📈 Si el volumen crece...

#### **Ingesta: Pandas → Polars**
- Ejecución paralela
- Menor consumo de memoria (Apache Arrow)
- Transparente para la lógica de negocio

#### **Desacoplamiento API ↔ Ingesta**
- API: validaciones + transacciones
- Jobs: procesamiento pesado
- Resultado: paralelización y reintentos controlados

#### **Migración Cloud-Native (GCP)**
- 🗄️ Cloud Storage para datos y backups
- ✅ Inmutabilidad y versionado mantenidos
- 📊 Servicios administrados para ingestas
- 🔍 Sin cambios en Data Quality ni trazabilidad