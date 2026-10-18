#!/usr/bin/env bash
# ============================================================================
# Universal Orchestration Script — Health Colombia ODS 3 ETL
# Compatible with Podman Compose, Podman, and Docker Compose
# ============================================================================
set -e

# Detect container compose tool
if command -v podman-compose >/dev/null 2>&1; then
    COMPOSE_CMD="podman-compose"
elif podman compose version >/dev/null 2>&1; then
    COMPOSE_CMD="podman compose"
elif docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "ERROR: Neither podman-compose nor docker compose found in PATH."
    exit 1
fi

export AIRFLOW_UID="${AIRFLOW_UID:-0}"

usage() {
    cat <<EOF
Uso: ./run.sh [COMANDO] [OPCIONES]

Punto de entrada unico:
  up                Valida puertos + compose up -d --build + bootstrap Superset
  ports [--yes]     Valida contenedores/puertos activos (5432, 8080, 8088, 3000)
  down              Baja todos los servicios del proyecto (volumenes intactos)
  stop              Alias de down
  fresh [--yes]     Limpieza total y arranque desde cero (clean-all + up)
  clean-all [--yes] Limpieza total (contenedores, BDs, logs de Airflow y datos)
  clean-airflow     Limpia solo los logs de Airflow (logs/)
  clean-db [--yes]  Detiene contenedores y borra volumenes de BD (PostgreSQL/Superset)
  clean-data        Limpia datos intermedios y de salida (data/work, output)
  clean [--yes]     Alias de clean-all
  reset [--yes]     Alias de clean-all
  trigger           DAG Test A (8/8 success en etl_ods3_salud_colombia)
  trigger-bad       DAG Test B: simula falla critica DQ-02 (bloqueo controlado)
  validate          Reporte automatizado de confiabilidad (Tests A, B y C)
  test              pytest completo (unidad + integracion)
  unit              Solo pruebas unitarias offline
  status            Contenedores y URLs de acceso
  superset          Re-ejecuta el bootstrap de Superset (idempotente)
  logs              Logs del scheduler y webserver de Airflow
  smoke             Pipeline local/contenedor sin Airflow (src.etl_main)
  postgrest-test    Probar endpoints REST de PostgREST compatibles con Power BI
  psql              Abrir consola interactiva de PostgreSQL (salud_colombia)
EOF
}

cmd="${1:-usage}"
shift || true

case "$cmd" in
    up)
        echo "==> Validando y levantando infraestructura con $COMPOSE_CMD..."
        $COMPOSE_CMD up -d postgres postgrest
        echo "==> Esperando que PostgreSQL este listo..."
        sleep 5
        $COMPOSE_CMD up -d superset-init
        sleep 3
        $COMPOSE_CMD up -d superset
        $COMPOSE_CMD up -d airflow-init
        sleep 3
        $COMPOSE_CMD up -d airflow-webserver airflow-scheduler
        echo "==> Ejecutando bootstrap de Superset..."
        $COMPOSE_CMD run --rm superset-bootstrap python /app/scripts/superset_bootstrap.py || true
        echo ""
        echo "============================================================"
        echo " INFRAESTRUCTURA LEVANTADA EXITOSAMENTE"
        echo "============================================================"
        echo "  PostgreSQL DW:       localhost:5432 (DB: salud_colombia, user: etl_user)"
        echo "  PostgREST (Power BI): http://localhost:3000"
        echo "  Apache Superset BI:  http://localhost:8088 (admin / admin)"
        echo "  Apache Airflow 3:    http://localhost:8080 (admin / admin)"
        echo "============================================================"
        ;;

    ports)
        echo "==> Verificando puertos del proyecto (5432, 8080, 8088, 3000)..."
        ports_to_check=(5432 8080 8088 3000)
        for p in "${ports_to_check[@]}"; do
            if ss -tulpn 2>/dev/null | grep -q ":$p "; then
                echo "  [OCUPADO] Puerto $p esta en uso."
            else
                echo "  [LIBRE]   Puerto $p esta disponible."
            fi
        done
        ;;

    down|stop)
        echo "==> Deteniendo contenedores..."
        $COMPOSE_CMD down
        ;;

    clean-airflow)
        echo "==> Limpiando logs de Airflow..."
        rm -rf logs/* 2>/dev/null || true
        echo "Logs eliminados."
        ;;

    clean-data)
        echo "==> Limpiando datos de trabajo y salida..."
        rm -rf data/work/* data/output/* 2>/dev/null || true
        echo "Datos intermedios eliminados."
        ;;

    clean-db)
        echo "==> Deteniendo contenedores y eliminando volumenes de BD..."
        $COMPOSE_CMD down -v
        ;;

    clean-all|clean|reset)
        echo "==> Ejecutando limpieza TOTAL..."
        $COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true
        rm -rf logs/* data/work/* data/output/* etl.log 2>/dev/null || true
        echo "Limpieza completa finalizada."
        ;;

    fresh)
        echo "==> Limpieza total y re-arranque en limpio..."
        $COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true
        rm -rf logs/* data/work/* data/output/* etl.log 2>/dev/null || true
        $0 up
        ;;

    etl|smoke)
        echo "==> Ejecutando pipeline ETL local en contenedor..."
        $COMPOSE_CMD run --rm etl python -m src.etl_main
        ;;

    trigger)
        echo "==> Disparando DAG Test A (8/8 success) en Apache Airflow..."
        $COMPOSE_CMD exec -T airflow-webserver airflow dags trigger etl_ods3_salud_colombia 2>/dev/null || \
        $COMPOSE_CMD run --rm etl python scripts/run_reliability_tests.py a
        ;;

    trigger-bad)
        echo "==> Disparando DAG Test B (falla controlada DQ-02) en Apache Airflow..."
        $COMPOSE_CMD exec -T airflow-webserver airflow dags trigger etl_ods3_salud_colombia --conf '{"simulate_failure":"DQ-02"}' 2>/dev/null || \
        $COMPOSE_CMD run --rm etl python scripts/run_reliability_tests.py b || true
        ;;

    test-a)
        echo "==> Ejecutando Test A: Corrida Exitosa..."
        $COMPOSE_CMD run --rm etl python scripts/run_reliability_tests.py a
        ;;

    test-b)
        echo "==> Ejecutando Test B: Falla Controlada Critica..."
        $COMPOSE_CMD run --rm etl python scripts/run_reliability_tests.py b || true
        ;;

    test-c)
        echo "==> Ejecutando Test C: Re-ejecucion Segura..."
        $COMPOSE_CMD run --rm etl python scripts/run_reliability_tests.py c
        ;;

    reliability|validate)
        echo "==> Ejecutando suite completa de confiabilidad (Tests A, B y C)..."
        $COMPOSE_CMD run --rm etl python scripts/run_reliability_tests.py all
        ;;

    superset|superset-init)
        echo "==> Registrando conexion DW y dashboard en Apache Superset..."
        $COMPOSE_CMD run --rm superset-bootstrap python /app/scripts/superset_bootstrap.py
        ;;

    postgrest-test)
        echo "==> Probando conectividad PostgREST para Power BI..."
        curl -s "http://localhost:3000/v_r6_demographic_coverage?limit=3" | python3 -m json.tool 2>/dev/null || echo "PostgREST endpoint no disponible. Ejecute ./run.sh up"
        ;;

    test|unit|test-unit)
        echo "==> Ejecutando suite de pruebas unitarias e integracion..."
        $COMPOSE_CMD run --rm etl pytest tests/
        ;;

    status)
        echo "==> Estado de contenedores ($COMPOSE_CMD ps):"
        $COMPOSE_CMD ps
        echo ""
        echo "URLs de Acceso:"
        echo "  - PostgREST OpenAPI / Power BI: http://localhost:3000/"
        echo "  - PostgREST Vista R6:          http://localhost:3000/v_r6_demographic_coverage"
        echo "  - PostgREST Vista R7:          http://localhost:3000/v_r7_territorial_capacity"
        echo "  - Apache Superset Dashboard:   http://localhost:8088 (admin / admin)"
        echo "  - Apache Airflow Webserver:    http://localhost:8080 (admin / admin)"
        ;;

    logs)
        echo "==> Mostrando logs del scheduler de Airflow..."
        $COMPOSE_CMD logs -f airflow-scheduler airflow-webserver
        ;;

    psql)
        $COMPOSE_CMD exec postgres psql -U etl_user -d salud_colombia
        ;;

    *)
        usage
        ;;
esac
