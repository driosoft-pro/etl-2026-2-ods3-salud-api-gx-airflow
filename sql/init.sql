-- =====================================================
-- DIMENSIONAL SCHEMA - HEALTH COLOMBIA
-- ODS 3 - Salud y Bienestar / Meta 3.8
--
-- Two fact tables with different granularities:
--   fact_affiliates: enrollment by geography/regime/quarter (Q2 2022 snapshot)
--   fact_facility_capacity: infrastructure by facility/type/geography (Q4 2022 snapshot)
--
-- Normalized schema (v3):
--   - dim_geografia is the SINGLE conformed geography dimension.
--     Contains DANE codes, municipality, department, region,
--     AND API Colombia enrichment (capital, surface, population).
--   - No redundant geographic tables.
--   - Both facts reference dim_geografia directly.
-- =====================================================

-- =====================================================
-- DROP EXISTING OBJECTS (idempotent reset)
-- =====================================================

DROP TABLE IF EXISTS fact_facility_capacity CASCADE;
DROP TABLE IF EXISTS fact_affiliates CASCADE;
DROP TABLE IF EXISTS dim_capacity_type CASCADE;
DROP TABLE IF EXISTS dim_facility CASCADE;
DROP TABLE IF EXISTS dim_regime CASCADE;
DROP TABLE IF EXISTS dim_geografia CASCADE;
DROP TABLE IF EXISTS dim_time CASCADE;
DROP VIEW IF EXISTS v_affiliates_summary CASCADE;
DROP VIEW IF EXISTS v_facility_summary CASCADE;

-- =====================================================
-- DIMENSIONS
-- =====================================================

-- Time Dimension (Trimestral)
CREATE TABLE dim_time (
    sk_time SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    semester INTEGER NOT NULL,
    periodo_codigo VARCHAR(10) NOT NULL,
    full_date DATE NOT NULL,
    UNIQUE(year, quarter)
);

-- =====================================================
-- UNIFIED GEOGRAPHY DIMENSION (single conformed dimension)
-- Both fact tables reference this dimension.
-- Contains DANE codes + API Colombia enrichment.
-- =====================================================
CREATE TABLE dim_geografia (
    sk_geografia SERIAL PRIMARY KEY,
    codigo_dane_municipio VARCHAR(10) NOT NULL,
    municipio VARCHAR(150) NOT NULL,
    codigo_dane_depto VARCHAR(5) NOT NULL,
    departamento VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    -- API Colombia enrichment columns
    capital VARCHAR(100),
    surface NUMERIC,
    population BIGINT,
    municipalities_count INTEGER,
    phone_prefix VARCHAR(10),
    region_api VARCHAR(100),
    UNIQUE(codigo_dane_municipio)
);

-- Regime Dimension
CREATE TABLE dim_regime (
    sk_regime SERIAL PRIMARY KEY,
    code VARCHAR(5) NOT NULL UNIQUE,
    description VARCHAR(100) NOT NULL
);

-- Facility Dimension (Healthcare Institution - IPS)
-- Uses sk_geografia FK (unified geography).
CREATE TABLE dim_facility (
    sk_facility SERIAL PRIMARY KEY,
    provider_code VARCHAR(20) NOT NULL,
    name VARCHAR(300) NOT NULL,
    nit VARCHAR(20),
    nature VARCHAR(20),
    care_level INTEGER,
    manager VARCHAR(200),
    address VARCHAR(300),
    email VARCHAR(200),
    phone VARCHAR(20),
    sk_geografia INTEGER NOT NULL REFERENCES dim_geografia(sk_geografia),
    UNIQUE(provider_code, sk_geografia)
);

-- Capacity Type Dimension
CREATE TABLE dim_capacity_type (
    sk_capacity_type SERIAL PRIMARY KEY,
    "group" VARCHAR(50) NOT NULL,
    description VARCHAR(100) NOT NULL,
    UNIQUE("group", description)
);

-- =====================================================
-- FACT TABLES
-- =====================================================

-- Fact: Affiliates (grain: quarter + geography + regime)
-- Row = total accumulated affiliates for a regime, geography, quarter and year
-- Data snapshot: April 2022 (Q2 2022)
CREATE TABLE fact_affiliates (
    sk_affiliate SERIAL PRIMARY KEY,
    sk_time INTEGER NOT NULL REFERENCES dim_time(sk_time),
    sk_geografia INTEGER NOT NULL REFERENCES dim_geografia(sk_geografia),
    sk_regime INTEGER NOT NULL REFERENCES dim_regime(sk_regime),
    numero_afiliados BIGINT NOT NULL,
    UNIQUE(sk_time, sk_geografia, sk_regime)
);

-- Fact: Facility capacity (grain: facility + capacity type + geography + snapshot)
-- Row = installed capacity for a facility, capacity type, and geography at a point in time
-- Data snapshot: November 2022 (Q4 2022)
-- NOT additive across time (infrastructure is a stock, not a flow)
CREATE TABLE fact_facility_capacity (
    sk_capacity SERIAL PRIMARY KEY,
    sk_time INTEGER NOT NULL REFERENCES dim_time(sk_time),
    sk_facility INTEGER NOT NULL REFERENCES dim_facility(sk_facility),
    sk_capacity_type INTEGER NOT NULL REFERENCES dim_capacity_type(sk_capacity_type),
    sk_geografia INTEGER NOT NULL REFERENCES dim_geografia(sk_geografia),
    capacity_amount INTEGER NOT NULL,
    UNIQUE(sk_time, sk_facility, sk_capacity_type)
);

-- =====================================================
-- INDEXES FOR OPTIMIZATION
-- =====================================================

CREATE INDEX idx_fact_affiliates_time ON fact_affiliates(sk_time);
CREATE INDEX idx_fact_affiliates_geografia ON fact_affiliates(sk_geografia);
CREATE INDEX idx_fact_affiliates_regime ON fact_affiliates(sk_regime);

CREATE INDEX idx_fact_capacity_time ON fact_facility_capacity(sk_time);
CREATE INDEX idx_fact_capacity_facility ON fact_facility_capacity(sk_facility);
CREATE INDEX idx_fact_capacity_type ON fact_facility_capacity(sk_capacity_type);
CREATE INDEX idx_fact_capacity_geografia ON fact_facility_capacity(sk_geografia);

CREATE INDEX idx_geografia_depto ON dim_geografia(codigo_dane_depto);
CREATE INDEX idx_geografia_region ON dim_geografia(region);
CREATE INDEX idx_geografia_departamento ON dim_geografia(departamento);
CREATE INDEX idx_facility_geografia ON dim_facility(sk_geografia);

-- =====================================================
-- SUMMARY VIEWS
-- =====================================================

-- View: Affiliates summary using unified geography
CREATE VIEW v_affiliates_summary AS
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

-- View: Facility summary using unified geography
CREATE VIEW v_facility_summary AS
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
