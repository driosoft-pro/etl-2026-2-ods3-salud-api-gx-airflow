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
# Load COMPOSE_PROJECT_NAME from .env if present (avoids numeric dir-name networks)
if [ -f .env ]; then
    _cpn=$(grep -E '^COMPOSE_PROJECT_NAME=' .env | head -1 | cut -d= -f2-)
    if [ -n "$_cpn" ]; then
        export COMPOSE_PROJECT_NAME="$_cpn"
    fi
fi
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-salud_colombia}"

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
  trigger           DAG Test A (8/8 success en reliable_health_pipeline)
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
  audit             Reporte de las ultimas corridas ETL (esquema audit)
EOF
}

# Runtime para inspeccionar contenedores (podman o docker)
case "$COMPOSE_CMD" in
    docker*) RUNTIME_BIN="docker" ;;
    *)       RUNTIME_BIN="podman" ;;
esac

cstate() {
    timeout 10 $RUNTIME_BIN inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.State.ExitCode}}' "$1" 2>/dev/null || echo "missing||"
}

dump_logs() {
    [ -n "$1" ] || return 0
    echo "--- ultimos logs de $1 ---"
    timeout 15 $RUNTIME_BIN logs --tail 60 "$1" 2>&1 | sed 's/^/    /' || true
    echo "--- fin de logs ---"
}

die_up() {
    echo ""
    echo "ERROR: $1" >&2
    timeout 15 $RUNTIME_BIN ps -a 2>/dev/null || true
    [ -n "$2" ] && dump_logs "$2" || true
    exit 1
}

# cwait <contenedor> <healthy|running|exited0> <timeout_s>
cwait() {
    local name="$1" cond="$2" limit="${3:-180}" waited=0 st
    echo "   esperando '$name' ($cond, max ${limit}s)..."
    while [ "$waited" -lt "$limit" ]; do
        st=$(cstate "$name")
        case "$st" in
            missing\|\|) die_up "el contenedor $name no existe (no fue creado)" "" ;;
        esac
        # Fail-fast: contenedor caido con error (p.ej. crash-loop por restart: always)
        if [ "$cond" != "exited0" ]; then
            case "$st" in
                stopped\|*\|[1-9]*|exited\|*\|[1-9]*)
                    die_up "el contenedor $name se detuvo con error ($st). Revisa los logs de abajo" "$name" ;;
            esac
        fi
        case "$cond" in
            healthy)  case "$st" in
                          running\|healthy\|*|running\|none\|*) return 0 ;;
                          running\|*)
                              # Healthcheck de podman (systemd) puede quedar en 'starting'
                              # para siempre en algunos hosts: sondeo directo por TCP
                              # (el servidor temporal de initdb NO escucha en TCP).
                              if timeout 10 $RUNTIME_BIN exec "$name" pg_isready -h 127.0.0.1 -U "${DB_USER_ETL:-etl_user}" >/dev/null 2>&1; then
                                  return 0
                              fi ;;
                      esac ;;
            running)  case "$st" in running\|*) return 0 ;; esac ;;
            exited0)  case "$st" in
                          exited\|*\|0) return 0 ;;
                          exited\|*)    die_up "el contenedor $name termino con error ($st)" "$name" ;;
                      esac ;;
        esac
        echo "     ...estado=$st (${waited}s)"
        sleep 3
        waited=$((waited + 3))
    done
    die_up "timeout de ${limit}s esperando '$name' (condicion: $cond, estado actual: $st)" "$name"
}

# cup <timeout_s> <servicios...>  -> compose up -d con tope de tiempo
# pg_prepare_volume: PostgreSQL usa un bind mount (./.pgdata) en vez de un volumen
# nombrado. Podman rootless hace chown de los volumenes nombrados nuevos al uid de
# la imagen (999); con userns keep-id el contenedor corre como TU uid y initdb
# falla ("could not change permissions"). El bind mount conserva tu dueno.
PGDATA_HOST_DIR="${PGDATA_HOST_DIR:-./.pgdata}"
export PGDATA_HOST_DIR
pg_prepare_volume() {
    mkdir -p "$PGDATA_HOST_DIR" && chmod 700 "$PGDATA_HOST_DIR" 2>/dev/null || true
}

# purge_stale: elimina contenedores "fantasma" (nombre en storage pero invisibles
# para 'podman ps') y volumenes con archivos de otro uid (rm normal falla).
CONTAINER_NAMES="warehouse_salud postgrest_salud superset_init_salud superset_salud airflow_init_salud airflow_webserver_salud airflow_scheduler_salud airflow_dag_processor_salud etl_process"
purge_stale() {
    [ "$RUNTIME_BIN" = "podman" ] || return 0
    $RUNTIME_BIN rm -f -t 2 $CONTAINER_NAMES >/dev/null 2>&1 || true
    $RUNTIME_BIN rm -f --storage $CONTAINER_NAMES >/dev/null 2>&1 || true
    if [ "$1" = "volumes" ]; then
        local v mp
        for v in "${COMPOSE_PROJECT_NAME}_pgdata" "${COMPOSE_PROJECT_NAME}_superset_home" pgdata superset_home; do
            mp=$($RUNTIME_BIN volume inspect -f '{{.Mountpoint}}' "$v" 2>/dev/null) || mp=""
            $RUNTIME_BIN volume rm -f "$v" >/dev/null 2>&1 || true
            [ -n "$mp" ] && [ -d "$mp" ] && $RUNTIME_BIN unshare rm -rf "$mp" 2>/dev/null || true
        done
        if [ -d "$PGDATA_HOST_DIR" ]; then
            $RUNTIME_BIN unshare rm -rf "$PGDATA_HOST_DIR" 2>/dev/null || rm -rf "$PGDATA_HOST_DIR" 2>/dev/null || true
        fi
        local base="$HOME/.local/share/containers/storage/volumes"
        for v in "${COMPOSE_PROJECT_NAME}_pgdata" "${COMPOSE_PROJECT_NAME}_superset_home"; do
            [ -d "$base/$v" ] && $RUNTIME_BIN unshare rm -rf "$base/$v" 2>/dev/null || true
        done
    fi
}

cup() {
    local limit="$1"
    shift
    case " $* " in *" postgres "*)
        # contenedor fantasma (nombre ocupado en storage pero invisible)
        [ "$(cstate warehouse_salud)" = "missing||" ] && [ "$RUNTIME_BIN" = "podman" ] && \
            $RUNTIME_BIN rm -f --storage warehouse_salud >/dev/null 2>&1 || true
        pg_prepare_volume ;;
    esac
    timeout --kill-after=15 "$limit" $COMPOSE_CMD up -d "$@" \
        || die_up "fallo o tardo mas de ${limit}s: $COMPOSE_CMD up -d $*" ""
}

# etl_run <timeout_s> <comando...>
# Ejecuta un comando en el contenedor 'etl'.
#  - --profile tools : el servicio 'etl' esta tras un profile; sin esto
#                      podman-compose responde "missing services [etl]".
#  - --no-deps      : SIN esto compose hace 'up' de las dependencias con
#                     remove_orphans=True y BORRA el resto del stack
#                     (postgrest, superset, airflow...).
etl_run() {
    local limit="$1" rc=0
    shift
    case "$(cstate warehouse_salud)" in
        running\|*) ;;
        *)
            echo "   PostgreSQL no esta corriendo: levantandolo..."
            cup 300 postgres
            cwait warehouse_salud healthy 180
            ;;
    esac
    timeout --kill-after=15 "$limit" $COMPOSE_CMD --profile tools run -T --no-deps --rm etl "$@" < /dev/null || rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "ERROR: fallo el contenedor 'etl' (codigo $rc, tope ${limit}s)." >&2
    fi
    return "$rc"
}

# Valores de la BD leidos de .env (sin exportar el password)
_env_val() { grep -E "^$1=" .env 2>/dev/null | head -1 | cut -d= -f2-; }
DB_USER_ETL="${POSTGRES_USER:-$(_env_val POSTGRES_USER)}"
DB_USER_ETL="${DB_USER_ETL:-etl_user}"
DB_NAME_ETL="${POSTGRES_DB:-$(_env_val POSTGRES_DB)}"
DB_NAME_ETL="${DB_NAME_ETL:-salud_colombia}"

cmd="${1:-usage}"
shift || true

case "$cmd" in
    up)
        echo "==> Validando y levantando infraestructura con $COMPOSE_CMD..."
        echo "==> [1/8] PostgreSQL DW (warehouse_salud)..."
        cup 300 postgres
        cwait warehouse_salud healthy 180
        echo "==> [2/8] PostgREST (postgrest_salud)..."
        cup 300 --no-deps postgrest
        cwait postgrest_salud running 90
        echo "==> [3/8] Superset init (one-off)..."
        cup 600 --no-deps superset-init
        cwait superset_init_salud exited0 600
        echo "==> [4/8] Apache Superset (8088)..."
        cup 300 --no-deps superset
        cwait superset_salud running 180
        echo "==> [5/8] Airflow init (one-off)..."
        cup 600 --no-deps airflow-init
        cwait airflow_init_salud exited0 600
        echo "==> [6/8] Airflow webserver + scheduler + dag-processor..."
        cup 300 --no-deps airflow-webserver airflow-scheduler airflow-dag-processor
        cwait airflow_webserver_salud running 180
        cwait airflow_scheduler_salud running 60
        cwait airflow_dag_processor_salud running 60
        echo "==> [7/8] Ejecutando bootstrap de Superset..."
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/superset_bootstrap.py || true
        echo "==> Registrando charts R1-R7 en el dashboard..."
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/superset_charts_bootstrap.py || true
        echo "==> Construyendo dashboards Granularidad y Workshop..."
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/dashboard/granularity.py || true
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/dashboard/workshop.py || true
        echo "==> Construyendo dashboard de Requerimientos (R1-R7 con tabs)..."
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/dashboard/requirements.py || true
        echo "==> [8/8] Pipeline ETL (carga del DW)..."
        rows=$($RUNTIME_BIN exec warehouse_salud psql -U "$DB_USER_ETL" -d "$DB_NAME_ETL" \
               -tAc "SELECT COUNT(*) FROM fact_affiliates" 2>/dev/null || echo "")
        case "$rows" in
            ''|*[!0-9]*|0)
                if ! etl_run 900 python -m src.etl_main; then
                    echo ""
                    echo "ERROR: la infraestructura esta ARRIBA pero el pipeline ETL fallo."
                    echo "       Reintenta con: ./run.sh etl"
                    exit 1
                fi
                ;;
            *)
                echo "   DW ya poblado ($rows filas en fact_affiliates): se omite la carga."
                ;;
        esac
        echo ""
        echo "============================================================"
        echo " PROYECTO LEVANTADO Y EJECUTADO EXITOSAMENTE"
        echo "============================================================"
        echo "  PostgreSQL DW:       localhost:5432 (DB: salud_colombia, user: etl_user)"
        echo "  Airflow metadata:    localhost:5432 (DB: airflow_meta — NO es el DW)"
        echo "  PostgREST (Power BI): http://localhost:3000"
        echo "  Apache Superset BI:  http://localhost:8088 (admin / admin)"
        echo "  Apache Airflow 3:    http://localhost:8080 (airflow / airflow)"
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
        $COMPOSE_CMD down --remove-orphans 2>/dev/null || true
        if [ "$RUNTIME_BIN" = "podman" ]; then
            $RUNTIME_BIN stop -t 2 warehouse_salud postgrest_salud superset_init_salud superset_salud airflow_init_salud airflow_webserver_salud airflow_scheduler_salud airflow_dag_processor_salud etl_process 2>/dev/null || true
            $RUNTIME_BIN rm -f warehouse_salud postgrest_salud superset_init_salud superset_salud airflow_init_salud airflow_webserver_salud airflow_scheduler_salud airflow_dag_processor_salud etl_process 2>/dev/null || true
            for scope in $(systemctl --user list-units --type=scope --state=running --no-legend 2>/dev/null | grep -E "libpod-|podman-" | awk '{print $1}'); do
                systemctl --user stop "$scope" 2>/dev/null || true
            done
        fi
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
        $COMPOSE_CMD down -v 2>/dev/null || true
        purge_stale volumes
        if [ "$RUNTIME_BIN" = "podman" ]; then
            $RUNTIME_BIN volume rm -f salud_colombia_pgdata salud_colombia_superset_home pgdata superset_home 2>/dev/null || true
            mkdir -p "$HOME/.local/share/containers/storage/volumes/.trash" 2>/dev/null || true
            if [ -d "$HOME/.local/share/containers/storage/volumes/salud_colombia_pgdata" ]; then
                mv "$HOME/.local/share/containers/storage/volumes/salud_colombia_pgdata" "$HOME/.local/share/containers/storage/volumes/.trash/salud_colombia_pgdata_$$" 2>/dev/null || true
            fi
        fi
        ;;

    clean-all|clean|reset)
        echo "==> Ejecutando limpieza TOTAL..."
        $COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true
        purge_stale volumes
        if [ "$RUNTIME_BIN" = "podman" ]; then
            $RUNTIME_BIN stop -t 2 warehouse_salud postgrest_salud superset_init_salud superset_salud airflow_init_salud airflow_webserver_salud airflow_scheduler_salud airflow_dag_processor_salud etl_process 2>/dev/null || true
            $RUNTIME_BIN rm -f warehouse_salud postgrest_salud superset_init_salud superset_salud airflow_init_salud airflow_webserver_salud airflow_scheduler_salud airflow_dag_processor_salud etl_process 2>/dev/null || true
            $RUNTIME_BIN volume rm -f salud_colombia_pgdata salud_colombia_superset_home pgdata superset_home 2>/dev/null || true
            mkdir -p "$HOME/.local/share/containers/storage/volumes/.trash" 2>/dev/null || true
            if [ -d "$HOME/.local/share/containers/storage/volumes/salud_colombia_pgdata" ]; then
                mv "$HOME/.local/share/containers/storage/volumes/salud_colombia_pgdata" "$HOME/.local/share/containers/storage/volumes/.trash/salud_colombia_pgdata_$$" 2>/dev/null || true
            fi
            for scope in $(systemctl --user list-units --type=scope --state=running --no-legend 2>/dev/null | grep -E "libpod-|podman-" | awk '{print $1}'); do
                systemctl --user stop "$scope" 2>/dev/null || true
            done
        fi
        rm -rf logs/* data/work/* data/output/* etl.log 2>/dev/null || true
        echo "Limpieza completa finalizada."
        ;;

    fresh)
        echo "==> Limpieza total y re-arranque en limpio..."
        $COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true
        purge_stale volumes
        rm -rf logs/* data/work/* data/output/* etl.log 2>/dev/null || true
        $0 up
        ;;

    etl|smoke)
        echo "==> Ejecutando pipeline ETL local en contenedor..."
        etl_run 900 python -m src.etl_main
        ;;

    trigger)
        echo "==> Disparando DAG Test A (8/8 success) en Apache Airflow..."
        $COMPOSE_CMD exec -T airflow-webserver airflow dags trigger reliable_health_pipeline 2>/dev/null || \
        etl_run 900 python scripts/run_reliability_tests.py a
        ;;

    trigger-bad)
        echo "==> Disparando DAG Test B (falla controlada DQ-02) en Apache Airflow..."
        $COMPOSE_CMD exec -T airflow-webserver airflow dags trigger bad_health_pipeline 2>/dev/null || \
        etl_run 900 python scripts/run_reliability_tests.py b || true
        ;;

    test-a)
        echo "==> Ejecutando Test A: Corrida Exitosa..."
        etl_run 900 python scripts/run_reliability_tests.py a
        ;;

    test-b)
        echo "==> Ejecutando Test B: Falla Controlada Critica..."
        etl_run 900 python scripts/run_reliability_tests.py b || true
        ;;

    test-c)
        echo "==> Ejecutando Test C: Re-ejecucion Segura..."
        etl_run 900 python scripts/run_reliability_tests.py c
        ;;

    reliability|validate)
        echo "==> Ejecutando suite completa de confiabilidad (Tests A, B y C)..."
        etl_run 1800 python scripts/run_reliability_tests.py all
        ;;

    superset|superset-init)
        echo "==> Registrando conexion DW, datasets, charts y dashboard en Apache Superset..."
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/superset_bootstrap.py
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/superset_charts_bootstrap.py
        echo "==> Construyendo dashboards Granularidad, Workshop y Requerimientos..."
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/dashboard/granularity.py
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/dashboard/workshop.py
        $COMPOSE_CMD exec -T superset env SUPERSET_URL=http://localhost:8088 python /app/scripts/dashboard/requirements.py
        ;;

    postgrest-test)
        echo "==> Probando conectividad PostgREST para Power BI..."
        curl -s "http://localhost:3000/v_r6_demographic_coverage?limit=3" | python3 -m json.tool 2>/dev/null || echo "PostgREST endpoint no disponible. Ejecute ./run.sh up"
        ;;

    test|unit|test-unit)
        echo "==> Ejecutando suite de pruebas unitarias e integracion..."
        etl_run 1800 pytest tests/
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
        echo "  - Apache Airflow Webserver:    http://localhost:8080 (airflow / airflow)"
        ;;

    logs)
        echo "==> Mostrando logs del scheduler, dag-processor y webserver de Airflow..."
        $COMPOSE_CMD logs -f airflow-scheduler airflow-dag-processor airflow-webserver
        ;;

    psql)
        $COMPOSE_CMD exec postgres psql -U etl_user -d salud_colombia
        ;;

    audit)
        echo "==> Ultimas corridas ETL registradas (audit.etl_run):"
        $COMPOSE_CMD exec -T postgres psql -U etl_user -d salud_colombia -c \
            "SELECT run_id, run_type, status, stage_reached, dq_rules_passed || '/' || dq_rules_total AS dq, ROUND(duration_seconds,1) AS dur_s, started_at FROM audit.etl_run ORDER BY started_at DESC LIMIT 10;"
        echo ""
        echo "==> Ultimos lotes de carga al DW (audit.etl_batch_log):"
        $COMPOSE_CMD exec -T postgres psql -U etl_user -d salud_colombia -c \
            "SELECT batch_id, run_id, dag_id, status, strategy, target_rows_after, ROUND(duration_seconds,1) AS dur_s, started_at FROM audit.etl_batch_log ORDER BY started_at DESC LIMIT 10;"
        ;;

    *)
        usage
        ;;
esac
