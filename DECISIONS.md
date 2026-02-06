# DECISIONS.md – Architectural Decision Records

Este documento resume las decisiones técnicas tomadas durante el desarrollo del challenge
y el razonamiento detrás de cada una.

---

## 1. Uso de FastAPI para la API REST

**Decisión:** Se utilizó FastAPI como framework web.

**Motivo:**
- Alto rendimiento
- Tipado explícito
- Fácil integración con Pydantic
- Endpoints claros y auto-documentados

**Alternativas consideradas:** Flask, Django REST  
FastAPI resultó más adecuado para un servicio liviano de ingesta.

---

## 2. PostgreSQL como base de datos

**Decisión:** PostgreSQL como motor relacional.

**Motivo:**
- Soporte robusto para integridad referencial
- Buen manejo de JSON
- Funciones analíticas SQL
- Estándar en entornos productivos

---

## 3. Data Quality separada de la data válida

**Decisión:** Los registros inválidos no se descartan ni bloquean el proceso.

**Implementación:**
- Tabla `dq_rejections`
- Registro por fila rechazada
- Asociación mediante `run_id`

**Beneficio:**
- Auditoría
- Reprocesamiento
- Observabilidad real del pipeline

---

## 4. Batch Loading para ingesta histórica

**Decisión:** Carga por lotes en lugar de inserts fila a fila.

**Motivo:**
- Evita locks prolongados
- Reduce consumo de memoria
- Escalable para volúmenes mayores

---

## 5. AVRO para Backup & Recovery

**Decisión:** Backups en formato AVRO.

**Motivo:**
- Formato binario eficiente
- Incluye esquema
- Muy usado en data engineering

**Criterios cumplidos:**
- Inmutable
- Versionado
- Checksum SHA256

---

## 6. SQL separado para métricas

**Decisión:** Queries analíticas fuera de la API.

**Motivo:**
- Separación de responsabilidades
- Facilita revisión por analistas
- Reproducible y auditable

---

## 7. Docker y Docker Compose

**Decisión:** Contenerización completa del entorno.

**Motivo:**
- Reproducibilidad
- Onboarding rápido
- Alineado a prácticas modernas de plataforma
