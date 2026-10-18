"""Reliable batch pipeline for Health Colombia (ODS 3 - Second Delivery).
Airflow 3.1.8 / 2.x TaskFlow implementation with Great Expectations gates.

Architecture (Sections 16.1 & 17 of Second Delivery brief):

    Primary Branch                      Convergence & Target                 API Branch
    ==============                      ====================                 ==========
    CSV DATASETS                                                             API COLOMBIA
         |                                                                        |
    extract_primary                                                          extract_api
         |                                                                        |
    validate_primary_raw (GX Gate 1)      both raw gates satisfy policy      validate_api_raw (GX Gate 2)
                                                |
                                    transform_and_integrate
                                                |
                                     validate_prepared (GX Gate 3)
                                                |
                                       load_dimensional_dw
                                                |
                                      verify_dw_and_kpis
                                                |
                                    DATA WAREHOUSE & DASHBOARDS
                                    (PostgREST, Superset, Power BI)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

try:
    from airflow.sdk import dag, task, get_current_context, Param
except ImportError:
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context
    from airflow.models.param import Param

from src.extract import extract_all
from src.api_extract import extract_api_all
from src.transform import (
    clean_affiliates, clean_facilities,
    build_dim_time, build_dim_geografia,
    build_dim_regime, build_dim_facility, build_dim_capacity_type,
    build_fact_affiliates, build_fact_facility_capacity
)
from src.quality import (
    validate_primary_raw, validate_api_raw, validate_prepared_data,
    CriticalQualityError
)
from src.load import load_all, export_to_csv, get_connection

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "health-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # Deterministic tasks default to 0 retries
}

# Transient tasks (e.g. external network HTTP calls) have bounded retries
TRANSIENT_TASK_RETRY = {
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
}

def _get_simulation_param() -> str | None:
    try:
        context = get_current_context()
        dag_run = context.get("dag_run")
        if dag_run and dag_run.conf and "simulate_failure" in dag_run.conf:
            return str(dag_run.conf["simulate_failure"])
        params = context.get("params", {})
        if "simulate_failure" in params:
            val = str(params["simulate_failure"]).strip()
            return val if val and val.lower() != "none" else None
    except Exception:
        pass
    return None


@dag(
    dag_id="etl_ods3_salud_colombia",
    default_args=DEFAULT_ARGS,
    description="Orquestacion ETL Confiable ODS 3 Salud Colombia con Great Expectations y PostgREST",
    schedule=None,  # Triggered manually or by external schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "ods3", "salud", "gx", "airflow3", "second-delivery"],
    params={
        "simulate_failure": Param(
            default="None",
            type="string",
            description="Simular falla critica para Test B (Opciones: None, DQ-02, DQ-07, DQ-09)"
        ),
        "offline_api": Param(
            default=False,
            type="boolean",
            description="Usar cache local de API Colombia para ejecucion offline"
        ),
    }
)
def etl_ods3_salud_colombia_pipeline():

    @task(task_id="extract_primary")
    def task_extract_primary() -> Dict[str, Any]:
        """Extract primary CSV datasets (affiliates + facilities) and record batch metadata."""
        log.info("Task [extract_primary]: Loading raw healthcare datasets...")
        raw_data = extract_all()
        aff_count = len(raw_data['affiliates'])
        fac_count = len(raw_data['facilities'])
        log.info(f"Primary extraction complete: {aff_count} affiliates, {fac_count} facilities")
        return {
            "status": "extracted",
            "batch_id": f"primary_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            "affiliates_count": aff_count,
            "facilities_count": fac_count
        }

    @task(task_id="validate_primary_raw")
    def task_validate_primary_raw(extract_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Gate 1: Great Expectations raw validation on primary datasets.
        Evaluates DQ-01 to DQ-05. Critical violations block downstream tasks.
        """
        sim_fail = _get_simulation_param()
        log.info(f"Task [validate_primary_raw]: Executing GX Gate 1 (simulate_failure={sim_fail})...")
        raw_data = extract_all()
        result = validate_primary_raw(
            raw_data['affiliates'],
            raw_data['facilities'],
            simulate_failure=sim_fail
        )
        log.info(f"Gate 1 passed: {result['passed_rules']}/{result['total_rules']} rules satisfied")
        return {
            "status": "passed",
            "gate": "primary_raw",
            "passed_rules": result['passed_rules']
        }

    @task(task_id="extract_api", **TRANSIENT_TASK_RETRY)
    def task_extract_api() -> Dict[str, Any]:
        """Extract external API Colombia datasets (Departments + Regions) with retry policy."""
        log.info("Task [extract_api]: Extracting demographic data from API Colombia...")
        try:
            context = get_current_context()
            offline = bool(context.get("params", {}).get("offline_api", False))
        except Exception:
            offline = False
            
        api_data = extract_api_all(force_cache=offline)
        dept_count = len(api_data['departments'])
        reg_count = len(api_data['regions'])
        source = api_data['metadata']['departments']['source']
        log.info(f"API extraction complete: {dept_count} departments, {reg_count} regions (source={source})")
        return {
            "status": "extracted",
            "batch_id": f"api_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            "departments_count": dept_count,
            "regions_count": reg_count,
            "source": source
        }

    @task(task_id="validate_api_raw")
    def task_validate_api_raw(api_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Gate 2: Great Expectations raw validation on API Colombia data.
        Evaluates DQ-06 and DQ-07. Critical violations block integration.
        """
        sim_fail = _get_simulation_param()
        log.info(f"Task [validate_api_raw]: Executing GX Gate 2 (simulate_failure={sim_fail})...")
        api_data = extract_api_all(force_cache=True)
        result = validate_api_raw(
            api_data['departments'],
            api_data['regions'],
            simulate_failure=sim_fail
        )
        log.info(f"Gate 2 passed: {result['passed_rules']}/{result['total_rules']} rules satisfied")
        return {
            "status": "passed",
            "gate": "api_raw",
            "passed_rules": result['passed_rules']
        }

    @task(task_id="transform_and_integrate")
    def task_transform_and_integrate(primary_gate_res: Dict[str, Any], api_gate_res: Dict[str, Any]) -> Dict[str, Any]:
        """Convergence Task: Harmonize, clean, and integrate both branches per Integration Contract.
        Executes only when both raw gates satisfy their severity policy.
        """
        log.info("Task [transform_and_integrate]: Both raw branches validated. Starting transformation...")
        raw_primary = extract_all()
        raw_api = extract_api_all(force_cache=True)
        
        df_aff = clean_affiliates(raw_primary['affiliates'])
        df_fac = clean_facilities(raw_primary['facilities'])
        
        dim_time = build_dim_time(df_aff, df_fac)
        dim_geo = build_dim_geografia(
            df_aff, df_fac,
            api_departments=raw_api['departments'],
            api_regions=raw_api['regions']
        )
        dim_reg = build_dim_regime(df_aff)
        dim_fac = build_dim_facility(df_fac, dim_geo)
        dim_ct = build_dim_capacity_type(df_fac)
        
        fact_aff = build_fact_affiliates(df_aff, dim_time, dim_geo, dim_reg)
        fact_cap = build_fact_facility_capacity(df_fac, dim_time, dim_fac, dim_ct, dim_geo)
        
        dimensions = {
            'time': dim_time, 'geografia': dim_geo, 'regime': dim_reg,
            'facility': dim_fac, 'capacity_type': dim_ct
        }
        facts = {'affiliates': fact_aff, 'capacity': fact_cap}
        
        # Export processed CSV artifacts
        export_to_csv(dimensions, facts)
        
        log.info(f"Transformation complete. Facts: {len(fact_aff)} affiliates, {len(fact_cap)} capacity records")
        return {
            "status": "transformed",
            "dim_geografia_count": len(dim_geo),
            "dim_facility_count": len(dim_fac),
            "fact_affiliates_count": len(fact_aff),
            "fact_capacity_count": len(fact_cap)
        }

    @task(task_id="validate_prepared")
    def task_validate_prepared(transform_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Gate 3: Great Expectations prepared-data validation gate.
        Evaluates DQ-08 to DQ-12 before loading into PostgreSQL Data Warehouse.
        """
        sim_fail = _get_simulation_param()
        log.info(f"Task [validate_prepared]: Executing GX Gate 3 (simulate_failure={sim_fail})...")
        raw_primary = extract_all()
        raw_api = extract_api_all(force_cache=True)
        raw_total = int(raw_primary['affiliates']['num_persons'].astype(str).str.replace('.', '', regex=False).astype(int).sum())
        
        df_aff = clean_affiliates(raw_primary['affiliates'])
        df_fac = clean_facilities(raw_primary['facilities'])
        dim_time = build_dim_time(df_aff, df_fac)
        dim_geo = build_dim_geografia(df_aff, df_fac, api_departments=raw_api['departments'], api_regions=raw_api['regions'])
        dim_reg = build_dim_regime(df_aff)
        dim_fac = build_dim_facility(df_fac, dim_geo)
        dim_ct = build_dim_capacity_type(df_fac)
        fact_aff = build_fact_affiliates(df_aff, dim_time, dim_geo, dim_reg)
        fact_cap = build_fact_facility_capacity(df_fac, dim_time, dim_fac, dim_ct, dim_geo)
        
        dimensions = {'time': dim_time, 'geografia': dim_geo, 'regime': dim_reg, 'facility': dim_fac, 'capacity_type': dim_ct}
        facts = {'affiliates': fact_aff, 'capacity': fact_cap}
        
        result = validate_prepared_data(dimensions, facts, raw_total_persons=raw_total, simulate_failure=sim_fail)
        log.info(f"Gate 3 passed: {result['passed_rules']}/{result['total_rules']} rules satisfied")
        return {
            "status": "passed",
            "gate": "prepared_gate",
            "passed_rules": result['passed_rules']
        }

    @task(task_id="load_dimensional_dw")
    def task_load_dw(prepared_gate_res: Dict[str, Any]) -> Dict[str, Any]:
        """Load accepted and validated dimensional data into PostgreSQL Data Warehouse.
        Implements safe idempotent loading for safe reruns (Test C).
        """
        log.info("Task [load_dimensional_dw]: Loading accepted dimensional data to DW...")
        raw_primary = extract_all()
        raw_api = extract_api_all(force_cache=True)
        
        df_aff = clean_affiliates(raw_primary['affiliates'])
        df_fac = clean_facilities(raw_primary['facilities'])
        dim_time = build_dim_time(df_aff, df_fac)
        dim_geo = build_dim_geografia(df_aff, df_fac, api_departments=raw_api['departments'], api_regions=raw_api['regions'])
        dim_reg = build_dim_regime(df_aff)
        dim_fac = build_dim_facility(df_fac, dim_geo)
        dim_ct = build_dim_capacity_type(df_fac)
        fact_aff = build_fact_affiliates(df_aff, dim_time, dim_geo, dim_reg)
        fact_cap = build_fact_facility_capacity(df_fac, dim_time, dim_fac, dim_ct, dim_geo)
        
        dimensions = {'time': dim_time, 'geografia': dim_geo, 'regime': dim_reg, 'facility': dim_fac, 'capacity_type': dim_ct}
        facts = {'affiliates': fact_aff, 'capacity': fact_cap}
        
        load_all(dimensions, facts)
        log.info("Data Warehouse dimensional load completed successfully.")
        return {
            "status": "loaded",
            "loaded_at": datetime.now(timezone.utc).isoformat(),
            "target": "salud_colombia"
        }

    @task(task_id="verify_dw_and_kpis")
    def task_verify_dw(load_res: Dict[str, Any]) -> Dict[str, Any]:
        """Post-load verification: Test analytical views v_r6 and v_r7 and verify KPI outputs."""
        log.info("Task [verify_dw_and_kpis]: Querying DW views for R6 and R7...")
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), ROUND(AVG(cobertura_afiliacion_pct), 2) FROM v_r6_demographic_coverage;")
                r6_count, r6_avg_cov = cur.fetchone()
                
                cur.execute("SELECT COUNT(*), ROUND(AVG(camas_por_1000_hab), 2) FROM v_r7_territorial_capacity;")
                r7_count, r7_avg_beds = cur.fetchone()
                
            log.info(f"R6 View check: {r6_count} departments, average coverage: {r6_avg_cov}%")
            log.info(f"R7 View check: {r7_count} departments, average beds/1k hab: {r7_avg_beds}")
            
            return {
                "status": "verified",
                "r6_departments": r6_count,
                "r6_avg_coverage_pct": float(r6_avg_cov) if r6_avg_cov else 0.0,
                "r7_departments": r7_count,
                "r7_avg_beds_per_1000": float(r7_avg_beds) if r7_avg_beds else 0.0
            }
        finally:
            conn.close()

    # --- Pipeline Dependency Graph (Airflow 3 TaskFlow) ---
    meta_primary = task_extract_primary()
    gate1 = task_validate_primary_raw(meta_primary)

    meta_api = task_extract_api()
    gate2 = task_validate_api_raw(meta_api)

    # Convergence: requires both raw validation gates to succeed
    meta_transform = task_transform_and_integrate(gate1, gate2)

    # Prepared gate
    gate3 = task_validate_prepared(meta_transform)

    # Loading and KPI verification
    meta_load = task_load_dw(gate3)
    task_verify_dw(meta_load)


# Instantiate the DAG
dag_instance = etl_ods3_salud_colombia_pipeline()
