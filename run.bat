@echo off
REM ============================================================================
REM Universal Orchestration Script for Windows — Health Colombia ODS 3 ETL
REM ============================================================================

set COMPOSE_CMD=docker compose
where podman-compose >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set COMPOSE_CMD=podman-compose
)

if "%1"=="" goto usage
if "%1"=="up" goto up
if "%1"=="down" goto down
if "%1"=="stop" goto down
if "%1"=="fresh" goto fresh
if "%1"=="clean-all" goto clean_all
if "%1"=="clean" goto clean_all
if "%1"=="reset" goto clean_all
if "%1"=="clean-airflow" goto clean_airflow
if "%1"=="clean-data" goto clean_data
if "%1"=="clean-db" goto clean_db
if "%1"=="etl" goto etl
if "%1"=="smoke" goto etl
if "%1"=="trigger" goto trigger
if "%1"=="trigger-bad" goto trigger_bad
if "%1"=="test-a" goto test_a
if "%1"=="test-b" goto test_b
if "%1"=="test-c" goto test_c
if "%1"=="reliability" goto reliability
if "%1"=="validate" goto reliability
if "%1"=="superset" goto superset_init
if "%1"=="superset-init" goto superset_init
if "%1"=="postgrest-test" goto postgrest_test
if "%1"=="test" goto test_unit
if "%1"=="unit" goto test_unit
if "%1"=="test-unit" goto test_unit
if "%1"=="status" goto status
if "%1"=="logs" goto logs
if "%1"=="ports" goto ports

:usage
echo Uso: run.bat [COMANDO]
echo Comandos disponibles:
echo   up                Levantar toda la infraestructura y bootstrap de Superset
echo   ports             Verificar puertos activos
echo   down / stop       Detener contenedores
echo   fresh             Limpieza total y arranque desde cero
echo   clean-all         Limpieza total de contenedores, volumenes y datos
echo   clean-airflow     Limpiar logs de Airflow
echo   clean-data        Limpiar datos intermedios
echo   etl / smoke       Ejecutar pipeline ETL completo
echo   trigger           Disparar DAG Test A en Airflow
echo   trigger-bad       Disparar DAG Test B (falla controlada DQ-02)
echo   validate          Ejecutar suite completa de confiabilidad (Tests A, B y C)
echo   superset          Inicializar conexion y dashboard en Superset
echo   postgrest-test    Probar endpoints PostgREST
echo   test / unit       Ejecutar pruebas unitarias
echo   status            Estado de los contenedores
echo   logs              Ver logs de Airflow
goto end

:up
echo Levantando infraestructura con %COMPOSE_CMD%...
%COMPOSE_CMD% up -d postgres postgrest superset airflow-webserver airflow-scheduler
%COMPOSE_CMD% run --rm superset-bootstrap python /app/scripts/superset_bootstrap.py
goto end

:ports
netstat -ano | findstr "5432 8080 8088 3000"
goto end

:down
%COMPOSE_CMD% down
goto end

:clean_all
%COMPOSE_CMD% down -v
goto end

:clean_airflow
del /q /s logs\*.* 2>nul
goto end

:clean_data
del /q /s data\work\*.* 2>nul
del /q /s data\output\*.* 2>nul
goto end

:clean_db
%COMPOSE_CMD% down -v
goto end

:fresh
%COMPOSE_CMD% down -v
del /q /s logs\*.* 2>nul
del /q /s data\work\*.* 2>nul
goto up

:etl
%COMPOSE_CMD% run --rm etl python -m src.etl_main
goto end

:trigger
%COMPOSE_CMD% exec -T airflow-webserver airflow dags trigger reliable_health_pipeline
goto end

:trigger_bad
%COMPOSE_CMD% exec -T airflow-webserver airflow dags trigger bad_health_pipeline
goto end

:test_a
%COMPOSE_CMD% run --rm etl python scripts/run_reliability_tests.py a
goto end

:test_b
%COMPOSE_CMD% run --rm etl python scripts/run_reliability_tests.py b
goto end

:test_c
%COMPOSE_CMD% run --rm etl python scripts/run_reliability_tests.py c
goto end

:reliability
%COMPOSE_CMD% run --rm etl python scripts/run_reliability_tests.py all
goto end

:superset_init
%COMPOSE_CMD% run --rm superset-bootstrap python /app/scripts/superset_bootstrap.py
goto end

:postgrest_test
curl -s "http://localhost:3000/v_r6_demographic_coverage?limit=3"
goto end

:test_unit
%COMPOSE_CMD% run --rm etl pytest tests/
goto end

:status
%COMPOSE_CMD% ps
goto end

:logs
%COMPOSE_CMD% logs -f airflow-scheduler airflow-webserver
goto end

:end
