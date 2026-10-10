import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from .config import DB_CONFIG
import logging
import os

logger = logging.getLogger(__name__)

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def _convert_to_python_types(values: list) -> list:
    """Convert numpy types to native Python types for psycopg2 compatibility."""
    converted = []
    for row in values:
        new_row = []
        for val in row:
            if isinstance(val, (np.integer,)):
                new_row.append(int(val))
            elif isinstance(val, (np.floating,)):
                new_row.append(float(val) if not np.isnan(val) else None)
            elif isinstance(val, np.bool_):
                new_row.append(bool(val))
            elif isinstance(val, np.ndarray):
                new_row.append(val.tolist())
            else:
                new_row.append(val)
        converted.append(new_row)
    return converted

def reset_schema(conn):
    init_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'init.sql')
    with open(init_path, 'r') as f:
        sql = f.read()
    with conn.cursor() as cursor:
        cursor.execute(sql)
    logger.info("Schema reset from init.sql")

def load_dimension(conn, table_name: str, df: pd.DataFrame, pk_col: str):
    logger.info(f"Loading dimension {table_name}: {len(df)} records")
    
    with conn.cursor() as cursor:
        columns = list(df.columns)
        quoted = [f'"{c}"' for c in columns]
        values = df[columns].values.tolist()
        values = _convert_to_python_types(values)
        
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(quoted)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        execute_values(cursor, insert_sql, values)
        
        cursor.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', '{pk_col}'), (SELECT COALESCE(MAX({pk_col}), 1) FROM {table_name}))")
    
    logger.info(f"Dimension {table_name} loaded successfully")

def load_fact(conn, table_name: str, df: pd.DataFrame, pk_col: str):
    logger.info(f"Loading fact table {table_name}: {len(df)} records")
    
    with conn.cursor() as cursor:
        columns = [c for c in df.columns if c != pk_col]
        quoted = [f'"{c}"' for c in columns]
        values = df[columns].values.tolist()
        values = _convert_to_python_types(values)
        
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(quoted)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        execute_values(cursor, insert_sql, values)
    
    logger.info(f"Fact table {table_name} loaded successfully")

def load_all(dimensions: dict, facts: dict):
    conn = get_connection()

    try:
        reset_schema(conn)

        load_dimension(conn, 'dim_time', dimensions['time'], 'sk_time')
        load_dimension(conn, 'dim_geografia', dimensions['geografia'], 'sk_geografia')
        load_dimension(conn, 'dim_regime', dimensions['regime'], 'sk_regime')
        load_dimension(conn, 'dim_facility', dimensions['facility'], 'sk_facility')
        load_dimension(conn, 'dim_capacity_type', dimensions['capacity_type'], 'sk_capacity_type')
        
        load_fact(conn, 'fact_affiliates', facts['affiliates'], 'sk_affiliate')
        load_fact(conn, 'fact_facility_capacity', facts['capacity'], 'sk_capacity')
        
        conn.commit()
        logger.info("Load completed successfully")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during load: {e}")
        raise
    finally:
        conn.close()

def export_to_csv(dimensions: dict, facts: dict):
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    os.makedirs(output_dir, exist_ok=True)

    all_tables = {}
    for name, df in dimensions.items():
        all_tables[f"dim_{name}"] = df
    for name, df in facts.items():
        all_tables[f"fact_{name}"] = df

    for table_name, df in all_tables.items():
        path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"Exported {table_name}: {len(df)} records → {path}")
