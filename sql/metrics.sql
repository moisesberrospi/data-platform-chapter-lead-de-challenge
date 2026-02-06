-- ============================================
-- SQL METRICS - Technical Challenge
-- Motor: PostgreSQL
-- Tablas: departments, jobs, hired_employees
-- ============================================

-- ----------------------------
-- SANITY CHECKS (opcional)
-- ----------------------------
-- Verifica si hay data y cuántas filas caen en 2021
SELECT 'departments' AS table_name, COUNT(*) AS total_rows FROM departments
UNION ALL
SELECT 'jobs'        AS table_name, COUNT(*) AS total_rows FROM jobs
UNION ALL
SELECT 'hired_employees' AS table_name, COUNT(*) AS total_rows FROM hired_employees;

SELECT
  COUNT(*) AS hired_total,
  COUNT(*) FILTER (WHERE datetime >= timestamp '2021-01-01' AND datetime < timestamp '2022-01-01') AS hired_2021
FROM hired_employees;


-- ============================================
-- MÉTRICA A
-- Número de empleados contratados por cargo y departamento en 2021,
-- desglosado por trimestre.
-- ============================================
WITH hires_2021 AS (
  SELECT
    he.department_id,
    he.job_id,
    date_part('quarter', he.datetime)::int AS quarter
  FROM hired_employees he
  WHERE he.datetime >= timestamp '2021-01-01'
    AND he.datetime <  timestamp '2022-01-01'
)
SELECT
  d.department AS department,
  j.job        AS job,
  h.quarter    AS quarter,
  COUNT(*)     AS hired
FROM hires_2021 h
JOIN departments d ON d.id = h.department_id
JOIN jobs j        ON j.id = h.job_id
GROUP BY 1,2,3
ORDER BY 1,2,3;


-- ============================================
-- MÉTRICA B
-- Departamentos que contrataron más que la media de contrataciones
-- de todos los departamentos en 2021.
-- ============================================
WITH dept_hires_2021 AS (
  SELECT
    d.id,
    d.department,
    COUNT(*)::int AS hired_2021
  FROM hired_employees he
  JOIN departments d ON d.id = he.department_id
  WHERE he.datetime >= timestamp '2021-01-01'
    AND he.datetime <  timestamp '2022-01-01'
  GROUP BY d.id, d.department
),
avg_hires AS (
  SELECT AVG(hired_2021)::numeric AS avg_hired_2021
  FROM dept_hires_2021
)
SELECT
  dh.department,
  dh.hired_2021,
  a.avg_hired_2021
FROM dept_hires_2021 dh
CROSS JOIN avg_hires a
WHERE dh.hired_2021 > a.avg_hired_2021
ORDER BY dh.hired_2021 DESC, dh.department;
