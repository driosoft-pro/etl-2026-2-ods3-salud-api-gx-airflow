import pytest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "unit: marca para tests unitarios"
    )
    config.addinivalue_line(
        "markers", "integration: marca para tests de integración"
    )
    config.addinivalue_line(
        "markers", "data: marca para tests de validación de datos"
    )