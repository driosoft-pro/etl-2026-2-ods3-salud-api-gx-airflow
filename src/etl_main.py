import logging
import sys
import os
import pandas as pd
from .extract import extract_all
from .transform import (
    clean_affiliates, clean_facilities,
    build_dim_time, build_dim_geografia,
    build_dim_regime, build_dim_facility, build_dim_capacity_type,
    build_fact_affiliates, build_fact_facility_capacity
)
from .validate import run_all_validations, validate_raw_affiliates
from .load import load_all, export_to_csv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), '..', 'etl.log'))
    ]
)
logger = logging.getLogger(__name__)

def run_etl():
    logger.info("=" * 60)
    logger.info("ETL PROCESS STARTED - HEALTH COLOMBIA")
    logger.info("=" * 60)
    
    try:
        logger.info("PHASE 1: EXTRACTION")
        raw_data = extract_all()
        
        logger.info("PHASE 1.5: RAW VALIDATION")
        raw_errors = validate_raw_affiliates(raw_data['affiliates'])
        if raw_errors:
            for e in raw_errors:
                logger.error(f"  [raw_affiliates] {e}")
            raise ValueError(f"Raw validation failed with {len(raw_errors)} errors")
        logger.info("Raw validation passed")
        
        logger.info("PHASE 2: TRANSFORMATION")
        raw_total = pd.to_numeric(
            raw_data['affiliates']['num_persons'].str.replace('.', '', regex=False),
            errors='coerce'
        ).fillna(0).sum()
        
        df_affiliates = clean_affiliates(raw_data['affiliates'])
        df_facilities = clean_facilities(raw_data['facilities'])
        
        logger.info("Building dimensions...")
        dim_time = build_dim_time(df_affiliates, df_facilities)
        dim_geografia = build_dim_geografia(df_affiliates, df_facilities)
        dim_regime = build_dim_regime(df_affiliates)
        dim_facility = build_dim_facility(df_facilities, dim_geografia)
        dim_capacity_type = build_dim_capacity_type(df_facilities)
        
        logger.info("Building fact tables...")
        fact_affiliates = build_fact_affiliates(df_affiliates, dim_time, dim_geografia, dim_regime)
        fact_capacity = build_fact_facility_capacity(
            df_facilities, dim_time, dim_facility, dim_capacity_type, dim_geografia
        )
        
        logger.info("PHASE 2.5: VALIDATION")
        validations = run_all_validations(
            fact_affiliates, fact_capacity,
            dim_time, dim_geografia, dim_regime, dim_facility, dim_capacity_type,
            original_total=raw_total,
            raw_facility_count=df_facilities['provider_code'].nunique()
        )
        total_errors = sum(len(v) for v in validations.values())
        if total_errors > 0:
            logger.warning(f"Validation found {total_errors} issues (check logs)")
        
        dimensions = {
            'time': dim_time,
            'geografia': dim_geografia,
            'regime': dim_regime,
            'facility': dim_facility,
            'capacity_type': dim_capacity_type
        }
        
        facts = {
            'affiliates': fact_affiliates,
            'capacity': fact_capacity
        }
        
        logger.info("PHASE 3: LOADING")
        load_all(dimensions, facts)
        
        logger.info("PHASE 4: CSV EXPORT")
        export_to_csv(dimensions, facts)
        
        logger.info("=" * 60)
        logger.info("ETL PROCESS COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        print("\n LOAD SUMMARY:")
        print(f"   dim_time:              {len(dim_time)} records")
        print(f"   dim_geografia:         {len(dim_geografia)} records")
        print(f"   dim_regime:            {len(dim_regime)} records")
        print(f"   dim_facility:          {len(dim_facility)} records")
        print(f"   dim_capacity_type:     {len(dim_capacity_type)} records")
        print(f"   fact_affiliates:       {len(fact_affiliates)} records (quarterly)")
        print(f"   fact_capacity:         {len(fact_capacity)} records")
        print(f"   validations:           {total_errors} errors")
        print(f"   CSV export:            data/processed/ directory")
        
    except Exception as e:
        logger.error(f"ETL process error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_etl()
