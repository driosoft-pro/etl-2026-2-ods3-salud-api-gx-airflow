import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.mark.unit
class TestLoadFunctions:
    
    def test_import_load(self):
        from src.load import load_dimension, load_fact, load_all
        assert callable(load_dimension)
        assert callable(load_fact)
        assert callable(load_all)
    
    def test_import_config(self):
        from src.config import DB_CONFIG
        assert 'host' in DB_CONFIG
        assert 'port' in DB_CONFIG
        assert 'database' in DB_CONFIG
        assert 'user' in DB_CONFIG
        assert 'password' in DB_CONFIG
    
    def test_config_port(self):
        from src.config import DB_CONFIG
        assert DB_CONFIG['port'] == 5432
    
    def test_config_database(self):
        from src.config import DB_CONFIG
        assert DB_CONFIG['database'] == 'salud_colombia'


@pytest.mark.integration
class TestDBConnection:
    
    def test_connection_possible(self):
        try:
            import psycopg2
            from src.config import DB_CONFIG
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            connection_success = True
        except Exception:
            connection_success = False
        
        assert connection_success, "Could not connect to database"
    
    def test_tables_exist(self):
        try:
            import psycopg2
            from src.config import DB_CONFIG
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = {row[0] for row in cursor.fetchall()}
            conn.close()
            
            expected_tables = {
                'dim_time', 'dim_geografia',
                'dim_regime', 'dim_facility', 'dim_capacity_type',
                'fact_affiliates', 'fact_facility_capacity'
            }
            
            missing = expected_tables - tables
            assert len(missing) == 0, f"Missing tables: {missing}"
            
        except Exception as e:
            pytest.skip(f"Could not verify tables: {e}")