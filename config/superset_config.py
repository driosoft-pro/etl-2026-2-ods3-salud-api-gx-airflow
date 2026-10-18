import os

# Superset configuration for Health Colombia ODS 3
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'salud-colombia-ods3-secret-key-2026-secure')

# Database connection for Superset metadata (SQLite by default inside container or PostgreSQL)
SQLALCHEMY_DATABASE_URI = os.getenv(
    'SUPERSET_SQLALCHEMY_DATABASE_URI',
    'sqlite:////app/superset_home/superset.db'
)

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,
}

# Allow iframe and external embeddings if needed
ENABLE_PROXY_FIX = True
ROW_LIMIT = 50000
SUPERSET_WEBSERVER_PORT = 8088

# Default language.
# NOTE: BABEL_DEFAULT_LOCALE must be "en" and LANGUAGES must be non-empty,
# otherwise the SPA crashes on the login page (LanguagePicker reads
# languages[locale].flag) and the frontend cannot fetch a language pack
# (/superset/language_pack/<lang>/ requires auth and returns HTML on redirect).
BABEL_DEFAULT_LOCALE = "en"
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "es": {"flag": "es", "name": "Español"},
}
