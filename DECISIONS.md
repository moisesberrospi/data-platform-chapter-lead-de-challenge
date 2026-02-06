# DECISIONS.md – Architectural Decision Records

Este documento resume las decisiones técnicas tomadas durante el desarrollo del challenge
y el razonamiento detrás de cada una.

---
## Resumen
El challenge lo abordé como si fuera una mini data platform en producción, no solo como un ejercicio técnico.
Primero, en la ingesta histórica, implementé un proceso batch para cargar los CSV de departments, jobs y hired_employees hacia PostgreSQL. La carga no es fila a fila, sino por lotes, para evitar locks innecesarios y mejorar throughput. Además, agregué una capa explícita de Data Quality: los registros inválidos no se descartan, sino que se persisten en una tabla de rechazos con trazabilidad por run_id, motivo del error y data original. Esto permite auditoría, reprocesos y métricas de calidad.
Para la API REST, diseñé un endpoint único que recibe transacciones de 1 a 1000 registros para cualquiera de las tres tablas. Todas las validaciones de negocio se hacen en el servicio: tipos de datos, campos obligatorios e integridad referencial. En el caso de hired_employees, el servicio valida que los IDs de departamento y cargo existan antes de insertar, evitando empleados huérfanos. La idea fue centralizar reglas críticas y no depender de que los clientes hagan validaciones correctas.
En cuanto a gestión de esquema, utilicé Alembic para versionar la base de datos. Esto permitió evolucionar el modelo —por ejemplo, la tabla de Data Quality— sin resets manuales, manteniendo trazabilidad y consistencia entre entornos. Para mí, el esquema también es código y debe versionarse.
Luego implementé un framework de backup y recovery, donde cada tabla puede respaldarse en formato AVRO. Los backups son inmutables, versionados y con checksum, lo que permite restauraciones confiables y auditables. No es solo un dump, sino un artefacto de plataforma.
Para el análisis de datos, resolví las métricas solicitadas directamente en SQL optimizado y las dejé separadas de la API. Esto permite que cualquier analista pueda ejecutar las métricas sin depender del backend.
Finalmente, en arquitectura y observabilidad, la solución se ejecuta completamente en Docker Compose, con healthchecks, logs y trazabilidad por ejecución. Si una ingesta falla, el equipo puede saber cuándo falló, por qué y qué datos se rechazaron.
El diseño cubre el alcance actual, pero dejé documentada una evolución natural hacia mayores volúmenes, como el uso de Polars para procesamiento, desacoplar la ingesta del API y una migración cloud-native en GCP.
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

**Criterios cumplidos:**
- Inmutable
- Versionado

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

## 8. Propuesta de mejora y escalabilidad

La arquitectura actual cumple con los requerimientos del challenge y está pensada para ser clara, reproducible y observable. No obstante, si el volumen de datos creciera (por ejemplo, archivos de varios gigabytes o cargas recurrentes), se identifican las siguientes mejoras como evolución natural de la solución.

En la capa de ingesta, el procesamiento de archivos podría migrar de Pandas a Polars, aprovechando su ejecución paralela y menor consumo de memoria basado en Apache Arrow. Esto permitiría manejar archivos de mayor tamaño sin cambios significativos en la lógica de negocio.

Adicionalmente, la ingesta batch podría desacoplarse del servicio API, dejando al API enfocado únicamente en validaciones y recepción de transacciones, mientras que los procesos de carga pesada se ejecutan como jobs independientes, facilitando paralelización y reintentos controlados.

Desde el punto de vista de infraestructura, una migración a la nube permitiría utilizar almacenamiento en objetos para datos y backups (manteniendo inmutabilidad y versionado), así como servicios administrados para la ejecución de ingestas y análisis analítico, sin modificar el modelo de Data Quality ni la trazabilidad implementada.