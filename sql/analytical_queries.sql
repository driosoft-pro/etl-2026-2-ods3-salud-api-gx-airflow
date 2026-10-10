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
-- BONUS: API data enrichment queries (from dim_geografia)
-- =====================================================

-- Population by region (API data in dim_geografia)
SELECT
    g.region,
    SUM(g.population) AS total_population,
    COUNT(DISTINCT g.departamento) AS department_count
FROM dim_geografia g
WHERE g.population > 0
GROUP BY g.region
ORDER BY total_population DESC;

-- Departments with capital, surface, population
SELECT
    g.departamento,
    g.capital,
    g.surface,
    g.population,
    g.municipalities_count,
    g.region_api
FROM dim_geografia g
WHERE g.codigo_dane_municipio = g.codigo_dane_depto || '0000'
   OR LENGTH(g.codigo_dane_municipio) = 5
GROUP BY g.departamento, g.capital, g.surface, g.population,
         g.municipalities_count, g.region_api
ORDER BY g.departamento;
