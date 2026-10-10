import pandas as pd
import requests
import unicodedata
import logging

logger = logging.getLogger(__name__)

API_BASE = 'https://api-colombia.com/api/v1'

def _normalize(text: str) -> str:
    if not isinstance(text, str):
        return text
    import re
    nfkd = unicodedata.normalize('NFKD', text)
    result = ''.join(c for c in nfkd if not unicodedata.combining(c))
    result = re.sub(r'[.,]', '', result)
    return result.upper().strip()

def fetch_departments_from_api() -> pd.DataFrame:
    """Fetch department data from API Colombia and return a DataFrame."""
    url = f"{API_BASE}/Department"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records = []
        for dept in data:
            capital = dept.get('cityCapital', {})
            capital_name = capital.get('name', '') if isinstance(capital, dict) else ''
            records.append({
                'department_name_raw': dept.get('name', ''),
                'department_name': _normalize(dept.get('name', '')),
                'api_id': dept.get('id', None),
                'capital': capital_name,
                'surface': dept.get('surface', None),
                'population': dept.get('population', None),
                'municipalities_count': dept.get('municipalities', None),
                'phone_prefix': dept.get('phonePrefix', ''),
                'region_id': dept.get('regionId', None),
            })
        df = pd.DataFrame(records)
        logger.info(f"Fetched {len(df)} departments from API Colombia")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch departments from API: {e}")
        return pd.DataFrame(columns=[
            'department_name_raw', 'department_name', 'api_id',
            'capital', 'surface', 'population', 'municipalities_count',
            'phone_prefix', 'region_id'
        ])

def fetch_regions_from_api() -> pd.DataFrame:
    """Fetch region data from API Colombia."""
    url = f"{API_BASE}/Region"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        records = []
        for reg in data:
            records.append({
                'region_id': reg.get('id', None),
                'region_name': reg.get('name', ''),
            })
        df = pd.DataFrame(records)
        logger.info(f"Fetched {len(df)} regions from API Colombia")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch regions from API: {e}")
        return pd.DataFrame(columns=['region_id', 'region_name'])
