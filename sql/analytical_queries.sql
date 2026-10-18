-- =====================================================
-- ANALYTICAL QUERIES — HEALTH COLOMBIA ETL
-- ODS 3: Salud y Bienestar / Target 3.8
--
-- R1–R5 aligned with README Requirements Matrix
-- Uses unified dim_geografia dimension (v3 normalized schema).
-- =====================================================

-- =====================================================
-- R1: Total Affiliates by Regime Type
-- =====================================================

SELECT
    r.description AS regime,
    SUM(f.numero_afiliados) AS total_affiliates,
    ROUND(SUM(f.numero_afiliados) * 100.0 / SUM(SUM(f.numero_afiliados)) OVER(), 2) AS pct_share
FROM fact_affiliates f
JOIN dim_regime r ON f.sk_regime = r.sk_regime
GROUP BY r.description
ORDER BY total_affiliates DESC;

-- =====================================================
-- R2: Top and Bottom Departments by Affiliate Count
-- =====================================================

-- Top 10 departments
SELECT
    g.departamento AS department,
    SUM(f.numero_afiliados) AS total_affiliates
FROM fact_affiliates f
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
GROUP BY g.departamento
ORDER BY total_affiliates DESC
LIMIT 10;

-- Bottom 10 departments
SELECT
    g.departamento AS department,
    SUM(f.numero_afiliados) AS total_affiliates
FROM fact_affiliates f
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
GROUP BY g.departamento
ORDER BY total_affiliates ASC
LIMIT 10;

-- =====================================================
-- R3: Facilities by Nature and Care Level
-- =====================================================

SELECT
    fc.nature,
    fc.care_level,
    COUNT(DISTINCT fc.sk_facility) AS facility_count,
    SUM(c.capacity_amount) AS total_capacity
FROM fact_facility_capacity c
JOIN dim_facility fc ON c.sk_facility = fc.sk_facility
GROUP BY fc.nature, fc.care_level
ORDER BY fc.nature, fc.care_level;

-- =====================================================
-- R4: Installed Capacity per Affiliate by Department
-- Uses unified dim_geografia for both capacity and
-- affiliates aggregation.
-- =====================================================

WITH capacity_by_dept AS (
    SELECT
        g.departamento AS department,
        SUM(c.capacity_amount) AS total_capacity
    FROM fact_facility_capacity c
    JOIN dim_geografia g ON c.sk_geografia = g.sk_geografia
    WHERE g.departamento NOT IN ('SIN DEPARTAMENTO')
    GROUP BY g.departamento
),
affiliates_by_dept AS (
    SELECT
        g.departamento AS department,
        SUM(f.numero_afiliados) AS total_affiliates
    FROM fact_affiliates f
    JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
    WHERE g.departamento NOT IN ('SIN DEPARTAMENTO')
    GROUP BY g.departamento
)
SELECT
    COALESCE(a.department, c.department) AS department,
    COALESCE(c.total_capacity, 0) AS total_capacity,
    COALESCE(a.total_affiliates, 0) AS total_affiliates,
    ROUND(
        COALESCE(c.total_capacity, 0)::numeric /
        NULLIF(COALESCE(a.total_affiliates, 0), 0),
    4) AS capacity_per_affiliate
FROM affiliates_by_dept a
FULL OUTER JOIN capacity_by_dept c ON a.department = c.department
ORDER BY capacity_per_affiliate ASC;

-- =====================================================
-- R5: Regime Distribution by Geographic Region
-- =====================================================

SELECT
    g.region,
    r.description AS regime,
    SUM(f.numero_afiliados) AS total_affiliates,
    ROUND(SUM(f.numero_afiliados) * 100.0 / SUM(SUM(f.numero_afiliados)) OVER(PARTITION BY g.region), 2) AS pct_within_region
FROM fact_affiliates f
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
JOIN dim_regime r ON f.sk_regime = r.sk_regime
GROUP BY g.region, r.description
ORDER BY g.region, total_affiliates DESC;

-- =====================================================
-- BONUS: Summary Views for BI Dashboard
-- =====================================================

-- View: Affiliates summary with all dimension attributes
CREATE OR REPLACE VIEW v_affiliates_summary AS
SELECT
    g.departamento,
    g.municipio,
    g.region,
    g.codigo_dane_depto,
    g.codigo_dane_municipio,
    g.capital,
    g.surface,
    g.population,
    r.description AS regime,
    t.year,
    t.quarter,
    t.periodo_codigo,
    f.numero_afiliados
FROM fact_affiliates f
JOIN dim_time t ON f.sk_time = t.sk_time
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
JOIN dim_regime r ON f.sk_regime = r.sk_regime;

-- View: Facility capacity summary using unified geography
CREATE OR REPLACE VIEW v_facility_summary AS
SELECT
    g.departamento AS department,
    g.region AS department_region,
    g.municipio AS municipality,
    g.capital,
    g.surface,
    g.population,
    i.name AS facility,
    i.nature,
    ct."group" AS capacity_group,
    ct.description AS capacity_type,
    t.year,
    c.capacity_amount,
    g.codigo_dane_depto,
    g.codigo_dane_municipio
FROM fact_facility_capacity c
JOIN dim_time t ON c.sk_time = t.sk_time
JOIN dim_facility i ON c.sk_facility = i.sk_facility
JOIN dim_geografia g ON c.sk_geografia = g.sk_geografia
JOIN dim_capacity_type ct ON c.sk_capacity_type = ct.sk_capacity_type;

-- =====================================================
-- R6: Demographic Coverage & Healthcare Penetration Rate
-- Primary Source (fact_affiliates) + API Source (dim_geografia.population)
-- Demonstrates the real coverage rate of the SGSSS relative to official census population.
-- =====================================================

SELECT
    g.departamento,
    g.region,
    COALESCE(MAX(g.population), 0) AS poblacion_oficial_api,
    SUM(f.numero_afiliados) AS total_afiliados_sgsss,
    SUM(CASE WHEN r.description = 'SUBSIDIZED' THEN f.numero_afiliados ELSE 0 END) AS afiliados_subsidiado,
    SUM(CASE WHEN r.description = 'CONTRIBUTORY' THEN f.numero_afiliados ELSE 0 END) AS afiliados_contributivo,
    ROUND((SUM(f.numero_afiliados)::numeric / NULLIF(MAX(g.population), 0)) * 100, 2) AS cobertura_afiliacion_pct,
    (MAX(g.population) - SUM(f.numero_afiliados)) AS brecha_poblacional_no_afiliada,
    CASE
        WHEN MAX(g.population) IS NULL OR MAX(g.population) = 0 THEN 'Sin Dato Demografico'
        WHEN (SUM(f.numero_afiliados)::numeric / MAX(g.population)) >= 1.05 THEN 'Alta Concentracion / Movilidad'
        WHEN (SUM(f.numero_afiliados)::numeric / MAX(g.population)) >= 0.90 THEN 'Meta Universal Adecuada'
        ELSE 'Deficit de Cobertura'
    END AS categoria_cobertura
FROM fact_affiliates f
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
JOIN dim_regime r ON f.sk_regime = r.sk_regime
WHERE g.codigo_dane_depto != '00' AND g.departamento NOT IN ('SIN DEPARTAMENTO')
GROUP BY g.departamento, g.region
ORDER BY cobertura_afiliacion_pct DESC;

-- National summary for R6
SELECT
    SUM(f.numero_afiliados) AS total_afiliados_nacional,
    (SELECT SUM(sub.poblacion_depto) FROM (SELECT MAX(population) AS poblacion_depto FROM dim_geografia WHERE population > 0 GROUP BY codigo_dane_depto) sub) AS poblacion_nacional_api,
    ROUND(
        SUM(f.numero_afiliados)::numeric * 100.0 /
        NULLIF((SELECT SUM(sub.poblacion_depto) FROM (SELECT MAX(population) AS poblacion_depto FROM dim_geografia WHERE population > 0 GROUP BY codigo_dane_depto) sub), 0),
    2) AS tasa_cobertura_nacional_pct
FROM fact_affiliates f;

-- =====================================================
-- R7: Territorial Healthcare Capacity Density
-- Primary Source (fact_facility_capacity) + API Source (dim_geografia.population & surface)
-- Evaluates hospital beds per 1,000 inhabitants and capacity per square kilometer.
-- =====================================================

SELECT
    g.departamento,
    g.region,
    COALESCE(MAX(g.population), 0) AS poblacion_oficial_api,
    COALESCE(MAX(g.surface), 0) AS superficie_km2,
    COUNT(DISTINCT c.sk_facility) AS total_ips_habilitadas,
    SUM(CASE WHEN ct."group" ILIKE '%CAMA%' THEN c.capacity_amount ELSE 0 END) AS total_camas,
    SUM(CASE WHEN ct."group" ILIKE '%SALA%' THEN c.capacity_amount ELSE 0 END) AS total_salas,
    SUM(c.capacity_amount) AS capacidad_total_instalada,
    ROUND((SUM(CASE WHEN ct."group" ILIKE '%CAMA%' THEN c.capacity_amount ELSE 0 END)::numeric / NULLIF(MAX(g.population), 0)) * 1000, 2) AS camas_por_1000_habitantes,
    ROUND((SUM(c.capacity_amount)::numeric / NULLIF(MAX(g.surface), 0)) * 1000, 2) AS capacidad_por_1000_km2,
    ROUND(SUM(c.capacity_amount)::numeric / NULLIF(MAX(g.surface), 0), 4) AS densidad_territorial_infraestructura,
    CASE
        WHEN MAX(g.population) = 0 OR MAX(g.surface) = 0 THEN 'Sin Dato Base'
        WHEN (SUM(CASE WHEN ct."group" ILIKE '%CAMA%' THEN c.capacity_amount ELSE 0 END)::numeric / NULLIF(MAX(g.population), 0)) * 1000 >= 2.5 THEN 'Capacidad Hospitalaria Alta'
        WHEN (SUM(CASE WHEN ct."group" ILIKE '%CAMA%' THEN c.capacity_amount ELSE 0 END)::numeric / NULLIF(MAX(g.population), 0)) * 1000 >= 1.5 THEN 'Capacidad Hospitalaria Moderada'
        ELSE 'Deficit Hospitalario Critico'
    END AS nivel_alerta_hospitalaria
FROM fact_facility_capacity c
JOIN dim_geografia g ON c.sk_geografia = g.sk_geografia
JOIN dim_capacity_type ct ON c.sk_capacity_type = ct.sk_capacity_type
WHERE g.codigo_dane_depto != '00' AND g.departamento NOT IN ('SIN DEPARTAMENTO')
GROUP BY g.departamento, g.region
ORDER BY camas_por_1000_habitantes DESC;

-- View query verification (R6 & R7 Views for PostgREST & BI Dashboards)
SELECT * FROM v_r6_demographic_coverage LIMIT 10;
SELECT * FROM v_r7_territorial_capacity LIMIT 10;

