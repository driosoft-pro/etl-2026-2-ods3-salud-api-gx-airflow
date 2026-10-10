# Expo — ETL Health Colombia (ODS 3: Salud y Bienestar)

> Documento de exposición: qué es el proyecto completo, cómo ejecutarlo y guion para 4 personas.
> **Este archivo está en `.gitignore`** (material de la exposición, no se sube al repositorio).

---

## 1. Resumen ejecutivo

Construimos un **pipeline ETL completo** que transforma dos datasets públicos de salud colombiana en
un **data warehouse dimensional** (PostgreSQL + CSV) y lo alimenta con **5 consultas analíticas (R1–R5)**
alineadas al **ODS 3**. Sobre ese pipeline se montan las dos tecnologías de las actividades:

| Actividad | Tecnología | Qué aporta |
|---|---|---|
| **Activity 2** | Great Expectations **1.23.1** | 2 **puertas de calidad** con 50 expectations, política de severidad y evidencia JSON |
| **Activity 3** | Apache Airflow **3.1.8** | Un **DAG de 6 tareas** que orquesta las etapas, con dos gates, `retries=0` y una **corrida de fallo controlado** |

Ideas clave de la frase de apertura:

1. **No solo cargamos datos: los puertamos.** Ninguna fila llega al warehouse sin pasar dos gates.
2. **El gate manda sobre el orquestador.** Si la calidad dice `STOP`, Airflow falla la tarea y la
   carga **nunca corre**.
3. **Todo es verificable:** 104 pruebas, manifests por etapa y evidencia JSON de cada validación.

---

## 2. Problema, objetivos y requisitos (R1–R5)

**Problema:** Colombia tiene ~51 millones de afiliados al sistema de salud, pero la cobertura no es
homogénea: regiones periféricas concentran el régimen **Subsidiado** mientras la **Andina** concentra
el **Contributivo**, y la capacidad instalada (camas/salas) por afiliado varía ~4× entre
departamentos. Sin un almacén de datos único no se puede responder eso de forma auditable.

**Objetivo general:** construir un DW dimensional que permita medir afiliación, oferta de servicios
y su relación, con calidad de datos garantizada y reproducibilidad total.

| Req | Pregunta | KPI |
|---|---|---|
| **R1** | ¿Cuántos afiliados hay por régimen? | Total + % participación |
| **R2** | ¿Qué departamentos tienen más/menos afiliados? | Top 10 / Bottom 10 |
| **R3** | ¿Cuántos establecimientos hay por naturaleza y nivel de atención? | Conteo + capacidad |
| **R4** | ¿Cuánta capacidad instalada hay por afiliado? | `capacity_amount / numero_afiliados` por departamento |
| **R5** | ¿Cómo se distribuye el régimen por región geográfica? | % de régimen dentro de cada región |

---

## 3. Fuentes de datos

| Dataset | Granularidad | Filas | Uso |
|---|---|---|---|
| **Afiliados** (`affiliates_by_department_municipality_regime…csv`) | trimestre Q2-2022 × municipio × régimen | 3.369 | `fact_affiliates` |
| **Establecimientos / REPS** (`healthcare_facilities_by_level_capacity…csv`) | prestador × tipo de capacidad | 41.427 crudas → 10.921 prestadores únicos | `dim_facility` + `fact_facility_capacity` |

Perfilamiento hecho: nulos por columna, dominios de régimen (`S/C/E/I`), niveles de atención
(`1/2/3`), formatos de código DANE/mes/año, duplicados de REPS y ausencia de municipios.

---

## 4. Modelo dimensional (esquema estrella)

```
dim_time (2)                dim_regime (4)              dim_capacity_type (63)
dim_geografia (2.240)  ←──  dim_facility (10.921)
        │                         │
        ├── fact_affiliates (3.369) ────────── numero_afiliados
        └── fact_facility_capacity (31.496) ── capacity_amount
```

- **5 dimensiones, 2 hechos.** Dos hechos porque tienen **granos distintos**: afiliados se miden por
  *municipio × régimen × trimestre*; capacidad por *prestador × tipo de capacidad × geografía*.
- `dim_geografia` es **unificada** (municipios de ambos datasets) y enriquecida con la **API de
  Colombia**: capital, superficie, población, prefijo telefónico.
- Los municipios de establecimientos que no existen en afiliados reciben códigos DANE
  **sintéticos deterministas** (`hashlib.md5`) para no perder filas de REPS.
- Claves sustitutas `sk_*` + `UNIQUE` por clave natural; cargas con `ON CONFLICT DO NOTHING`.

---

## 5. Pipeline ETL (6 etapas, 1 CLI)

```bash
uv run python -m src.etl_main --stages extract validate_raw transform validate_prepared load summary
```

| Etapa | Qué hace | Evidencia |
|---|---|---|
| `extract` | Lee las fuentes (sustituibles por env `ETL_*_FILE`), bloquea columnas, sanea | `manifest_extract.json` |
| `validate_raw` | **Puerta 1** Great Expectations (18 rules) | `logs/gx/raw_*.json` |
| `transform` | Limpieza y construcción de dimensiones/hechos | `manifest_transform.json` |
| `validate_prepared` | **Puerta 2** Great Expectations (32 rules) | `logs/gx/prepared_*.json` |
| `load` | Resetea esquema (`init.sql`) y carga a PostgreSQL + CSV | `manifest_load.json` |
| `summary` | Exporta R1–R5 a `data/output/*.csv` | `manifest_summary.json` |

Correcciones de calidad ya aplicadas (documentadas en el README §14.2): distritos especiales de
salud mapeados a sus departamentos, `NO APLICA` → "SIN DEPARTAMENTO" (1.315.701 afiliados), CASANARE
con código DANE 85, deduplicación de REPS (13.970 claves repetidas), región calculada **después** de
normalizar departamentos y códigos deterministas con `md5`.

---

## 6. Calidad de datos — Great Expectations 1.23.1 (Activity 2)

**50 expectations** en `src/quality/rules.py`, ejecutadas por `src/quality/gx_gate.py`:

| Puerta | Antes de | Cobertura | Reglas |
|---|---|---|---|
| `validate_raw` | `transform` | afiliados (`HQ-A01`–`A08`) + establecimientos (`HQ-F01`–`F10`) | 18 |
| `validate_prepared` | `load` | 7 tablas (SK únicos/no nulos, FK completas, rangos de medidas, dominios) | 32 |

Cada puerta materializa **Suite + Validation Definition + Checkpoint** (sin Actions: la respuesta la
da el orquestador) y aplica la **política de severidad**:

| Resultado | Política | Efecto |
|---|---|---|
| Sin fallos | `CONTINUE` | sigue adelante |
| Solo fallos `warning` | `CONTINUE WITH WARNING` | sigue con advertencia en log |
| Algún fallo `critical` | `STOP` | `QualityGateError` → **falla la tarea, DAG Run detenido** |
| Expectation no evaluable | `STOP` | sin evidencia, la puerta no se abre |

**Evidencia:** `logs/gx/<etapa>_<timestamp UTC>.json` + `<etapa>_latest.json` y
`data/work/manifest_<etapa>.json`.

---

## 7. Orquestación — Apache Airflow 3.1.8 (Activity 3)

`dags/health_reliable_pipeline.py` (TaskFlow, `from airflow.sdk import dag, task`):

```
extract_raw_health_data → validate_raw_gate → transform_health_data
                       → validate_prepared_gate → load_warehouse → build_r1_r5_summary
```

- `schedule=None` + `catchup=False` → **DAG Runs manuales** (se inspecciona cada corrida).
- `retries=0` → las validaciones son deterministas, **reintentar no repara datos malos**.
- Los datos viajan como **archivos intermedios** en `data/work/`; en XCom solo circulan manifests.
- `_apply_run_conf()` mapea `dag_run.conf` → variables de entorno (`affiliates_file`, `facilities_file`,
  `target`, `gx_results_dir`), lo que habilita la **corrida de fallo controlado**.

**Stack en `docker-compose.yml` (compatible con `podman compose`):**
`postgres` (warehouse :5432), `pgadmin` (:5050), `etl` (por lotes), `airflow-postgres` (:5433),
`airflow-init` (`airflow db migrate`), `dag-processor`, `scheduler` (LocalExecutor), `triggerer`,
`api-server` (:8080, SimpleAuthManager sin credenciales).

---

## 8. Cómo ejecutarlo

### 8.0 Orquestador (recomendado) — `run.sh` / `run.bat`

Un solo script arranca el proyecto **paso a paso** (menú), lo ejecuta **todo
automático** y **informa del estado** de cada ejecución (contenedores, DAG
Runs, puertas GX, manifests y warehouse):

```bash
./run.sh              # menú interactivo (13 opciones)
./run.sh auto         # 1→7 en orden + resumen OK/ERR + informe de estado
./run.sh auto --demo  # idem + DAG con fallo controlado (la puerta debe cerrar)
./run.sh status       # solo informe de estado
./run.sh help         # comandos: doctor setup db etl tests up dag dag-fail status repair down
```

En Windows la misma receta con `run.bat` (Docker Desktop o Podman + Python
3.13 + `uv`). Los pasos del menú: requisitos, entorno, base de datos, ETL,
pruebas, stack Airflow, DAG Run, fallo controlado, estado, AUTO, reparar y
parar. Detalle completo en `README.md` §18 «Option 0».

### 8.1 Requisitos

- **Python 3.13** + [`uv`](https://docs.astral.sh/uv/) — *o* Nix (`nix develop`)
- **Podman + podman-compose** (Linux) o Docker Desktop (Windows) para la pila completa
- PostgreSQL 15+ si corres en local (o usa el contenedor `postgres`)

### 8.2 Opción A — local con uv (la más rápida)

```bash
uv sync --all-groups                 # instala desde uv.lock (Python 3.13)
psql -U postgres -f sql/init.sql     # esquema (o déjalo al load)
uv run python -m src.etl_main        # pipeline completo + 2 puertas
uv run python -m src.etl_main --target csv          # sin base de datos
uv run pytest                        # 104 pruebas (102 si no hay warehouse)
```

### 8.3 Opción B — Nix

```bash
nix develop          # Python 3.13 + uv (ejecuta uv sync) + PostgreSQL 16 + Podman
uv run python -m src.etl_main
```

### 8.4 Opción C — Podman/Docker (pipeline por lotes)

```bash
podman-compose up -d postgres        # warehouse (init.sql automático)
podman-compose run --rm etl          # pipeline completo; exit 0 = todo OK
```

### 8.5 Opción D — Podman/Docker con Airflow (la demo completa)

```bash
podman-compose up -d postgres airflow-init     # init corre `airflow db migrate`
podman-compose up -d                           # dag-processor, scheduler, triggerer, api-server
# UI: http://localhost:8080  (sin credenciales, SimpleAuthManager)

# Corrida feliz (DAG despausado por defecto tras el primer trigger)
podman exec airflow_api_server airflow dags unpause health_reliable_pipeline
podman exec airflow_api_server airflow dags trigger health_reliable_pipeline

# Corrida con fallo controlado (Activity 3, paso 20)
podman exec airflow_api_server airflow dags trigger health_reliable_pipeline \
  --conf '{"affiliates_file": "affiliates_bad.csv"}'

podman-compose logs -f scheduler
podman-compose down                                 # parar (volumes conservados)
```

### 8.6 Puertos y servicios

| Puerto | Servicio |
|---|---|
| `5432` | Warehouse PostgreSQL (`salud_colombia` / `etl_user`) |
| `5433` | Metadata DB de Airflow |
| `5050` | pgAdmin (`PGADMIN_LISTEN_PORT=5050`) |
| `8080` | Airflow UI / API / execution API |

### 8.7 Si algo falla (troubleshooting)

| Síntoma | Causa | Solución |
|---|---|---|
| `ModuleNotFoundError: numpy` en Nix | falta `LD_LIBRARY_PATH` | usar `nix develop` (ya lo define el flake) |
| Task de Airflow muere con `Connection refused` | task no alcanza la execution API | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://api-server:8080/execution/` (ya en compose) |
| `403 Signature verification failed` | JWT firmado con claves distintas por contenedor | `AIRFLOW__API_AUTH__JWT_SECRET` compartido (ya en compose) |
| pgAdmin `Permission denied` en puerto 80 | imagen corre como uid 5050 | `PGADMIN_LISTEN_PORT=5050` (ya en compose) |
| `bigint out of range` en `load` | `float('nan')` llega a una columna entera | normalizado a `NULL` en `src/load.py` (ya corregido) |

---

## 9. Resultados verificados (lo que se muestra en la expo)

### 9.1 Escenarios ejecutados

| Escenario | Comando | Resultado |
|---|---|---|
| Corrida completa local | `uv run python -m src.etl_main` | **exit 0**; puertas `CONTINUE` (18 + 32) |
| Corrida en contenedor | `podman-compose run --rm etl` | **exit 0** |
| Airflow camino feliz | `dags trigger health_reliable_pipeline` | run **success**, **6/6 tareas** |
| Fallo controlado | trigger con `affiliates_bad.csv` | `validate_raw_gate` **failed** (`HQ-A04` + `HQ-A06` críticas), resto **upstream_failed**, run **failed** |
| Pruebas | `uv run pytest` | **104 passed** |
| Imágenes | `podman build -f Dockerfile … / -f Dockerfile.airflow …` | ambas construyen |

### 9.2 R1 — Afiliados por régimen (total 51.182.238)

| Régimen | Afiliados | % |
|---|---|---|
| Subsidiado | 24.599.910 | 48,06 % |
| Contributivo | 24.341.821 | 47,56 % |
| Especial | 2.153.767 | 4,21 % |
| Individual | 86.740 | 0,17 % |

### 9.3 R2 — Extremos departamentales

- **Top 3:** Bogotá D.C. 8.120.311 · Antioquia 6.876.866 · Valle del Cauca 4.622.750
- **Bottom 3:** Vaupés 33.662 · Guainía 50.608 · San Andrés 62.879

### 9.4 R3 — Establecimientos por naturaleza y nivel

- **Privada:** 9.632 (9.631 sin nivel declarado)
- **Pública:** 1.274 (nivel 1: 1.038 · nivel 2: 193 · nivel 3: 34 · sin nivel: 9)
- **Mixta:** 15

### 9.5 R4 — Capacidad por afiliado

- **Mínimo:** Vaupés 0,0015 · **Máximo:** Sucre 0,0059 → **~4× de brecha**
- Bogotá 0,0047 · Antioquia 0,0037 · Valle 0,0045

### 9.6 R5 — Régimen por región

| Región | Subsidiado | Contributivo |
|---|---|---|
| Amazonia | **79,15 %** | 18,27 % |
| Caribe | **68,61 %** | 29,47 % |
| Orinoquia | **63,70 %** | 34,41 % |
| Pacifico | **56,82 %** | 41,46 % |
| Andina | 35,67 % | **62,49 %** |
| Insular | 26,38 % | **72,12 %** |

### 9.7 Warehouse

| Tabla | Registros |
|---|---|
| dim_time | 2 |
| dim_geografia | 2.240 |
| dim_regime | 4 |
| dim_facility | 10.921 |
| dim_capacity_type | 63 |
| fact_affiliates | 3.369 |
| fact_facility_capacity | 31.496 |

Validación final: **0 errores**, suma de afiliados conciliada al **100 %** (51.182.238).

---

## 10. Estructura del repositorio

```
├── pyproject.toml / uv.lock        # fuente de verdad de dependencias (uv, Python 3.13)
├── requirements.txt                # lista pip (imágenes Docker)
├── flake.nix / flake.lock          # entorno Nix (uv + Podman + PostgreSQL)
├── Dockerfile                      # imagen del ETL (python:3.13-slim)
├── Dockerfile.airflow              # imagen de Airflow 3.1.8 con src/ y dags/
├── docker-compose.yml              # warehouse + pgAdmin + Airflow completo
├── dags/health_reliable_pipeline.py
├── src/
│   ├── extract.py  transform.py  validate.py  load.py
│   ├── pipeline.py                 # etapas, manifests, lectura de intermedios
│   ├── etl_main.py                 # CLI (--stages / --target)
│   ├── summary.py                  # R1–R5 → data/output
│   └── quality/rules.py + gx_gate.py   # 50 expectations + política
├── sql/init.sql                    # esquema estrella (DROP/CREATE idempotente)
├── tests/                          # 104 pruebas (incluye calidad y pipeline)
├── README.md / README_ES.md        # informe completo
└── data/{raw,work,output,processed}
```

---

## 11. Limitaciones y supuestos

1. **Corte temporal:** afiliados Q2-2022 y establecimientos Q4-2022 (foto, no serie histórica).
2. **Nivel de atención:** ~61 % de establecimientos sin nivel declarado (queda `NULL`).
3. **Afiliación ≠ acceso:** el dataset mide inscripción al sistema, no uso real de servicios.
4. **Capacidad:** `capacity_amount` es capacidad *instalada*, no operativa.
5. **`dim_geografia` duplicada a propósito:** los códigos sintéticos de 6 dígitos conviven con los
   reales de 5 (característica v3, documentada).

---

## 12. Guion para 4 personas (~16 minutos + demo)

### Cronograma

| Tiempo | Persona | Bloque |
|---|---|---|
| 0:00 – 3:30 | **P1** | Contexto, problema ODS 3, fuentes y requisitos R1–R5 |
| 3:30 – 7:30 | **P2** | Datos, perfilamiento y **calidad (Great Expectations)** + demo del gate |
| 7:30 – 11:30 | **P3** | Modelo dimensional, pipeline ETL y **resultados R1–R5** |
| 11:30 – 15:30 | **P4** | **Airflow**, cómo se ejecuta, demo (corrida feliz + fallo controlado), pruebas y cierre |
| 15:30 – 16:00 | todos | Cierre de una frase + preguntas |

> Antes de empezar (1 min, fuera de tiempo): tener la UI de Airflow abierta en `localhost:8080`,
> `data/output/` abierto en el explorador y esta página en pantalla.

---

### P1 — Contexto y problema (3,30 min)

**Qué dice:**

1. *"Somos ETL Health Colombia: convertimos dos datasets públicos del sistema de salud en un data
   warehouse dimensional que responde 5 preguntas del ODS 3."*
2. *"El problema: 51 millones de afiliados, pero con patrones opuestos entre regiones — la Andina es
   mayoritariamente contributiva y las regiones periféricas, subsidiadas — y hasta 4 puntos de
   brecha en capacidad por afiliado. Sin un almacén único y auditado no se puede demostrar eso."*
3. Muestra las dos fuentes y su granularidad: **afiliados** (municipio × régimen × trimestre,
   3.369 filas) y **establecimientos/REPS** (prestador × capacidad, 41.427 filas crudas).
4. Enumera **R1–R5** con una línea cada uno y cierra: *"todo lo que sigue existe para responder
   esos cinco requisitos con evidencia."*

**Transición:** *"Para eso primero hay que entender la calidad de los datos — les pasa a P2."*

**Cue visual:** gráfico/pantalla del problema (README §2) o las tablas de R1–R5 en `data/output/`.

---

### P2 — Datos, calidad y Great Expectations (4 min)

**Qué dice:**

1. Perfilamiento rápido: dominios (régimen `S/C/E/I`, nivel `1/2/3`), nulos, formatos DANE,
   duplicados de REPS, municipios faltantes.
2. *"No validamos con `assert` sueltos: usamos **Great Expectations 1.23.1** con **50 reglas**
   declaradas una sola vez en `src/quality/rules.py`."*
3. Explica las **dos puertas** (18 antes de transformar, 32 antes de cargar) y la **política**:
   `CONTINUE` / `CONTINUE WITH WARNING` / **`STOP`** (+ no evaluable = STOP).
4. *"Cada corrida deja evidencia: JSON con timestamp en `logs/gx/` y un manifest por etapa en
   `data/work/`."*
5. **Demo (60–90 s):** dispara la corrida con datos corruptos y muestra que la puerta cierra.

```bash
ETL_AFFILIATES_FILE=affiliates_bad.csv uv run python -m src.etl_main --stages extract validate_raw
echo "exit=$?"        # esperado: 2
```

Línea a resaltar en pantalla:
`[raw] politica=STOP max_severity=critical fallos_critical=2 expectations=18`
y el manifest: `"status": "failed"` con `"failed_rules": ["HQ-A04", "HQ-A06"]`.

> **Plan B (si no hay tiempo/red):** mostrar el archivo ya generado
> `data/work/manifest_validate_raw.json` y `logs/gx/raw_latest.json`.

**Transición:** *"La puerta da luz verde… ahora sí construimos el modelo — les toca a P3."*

---

### P3 — Modelo dimensional, pipeline y resultados (4 min)

**Qué dice:**

1. **Esquema estrella:** 5 dimensiones (`time`, `geografia`, `regime`, `facility`, `capacity_type`)
   y **2 hechos**. *"Son dos porque el grano es distinto: afiliados por municipio×régimen×trimestre
   y capacidad por prestador×tipo×geografía."*
2. Enriquecimiento de `dim_geografia` con la **API de Colombia** (capital, superficie, población) y
   códigos DANE **sintéticos con `md5`** para no perder establecimientos de municipios ausentes.
3. Las **6 etapas** del pipeline con sus evidencias y las correcciones de calidad hechas
   (distritos especiales, `NO APLICA`, CASANARE, deduplicación, regiones).
4. **Resultados (los números mandan):**
   - R1: **48,06 % subsidiado / 47,56 % contributivo** (total 51.182.238, conciliación 100 %)
   - R2: Bogotá 8,12 M arriba; Vaupés 33.662 abajo
   - R3: 10.921 prestadores — privada 9.632, pública 1.274, mixta 15
   - R4: brecha **0,0015 (Vaupés) → 0,0059 (Sucre)**
   - R5: Amazonia **79,15 %** subsidiado vs Andina **62,49 %** contributivo
5. Muestra `data/output/*.csv` y `sql/analytical_queries.sql`.

**Cue visual:** `data/output/r1…r5*.csv` o el dashboard de Power BI.

**Transición:** *"Ahora todo eso se orquesta con Airflow — P4."*

---

### P4 — Airflow, ejecución y cierre (4 min)

**Qué dice:**

1. **El DAG:** 6 tareas TaskFlow, `schedule=None` (corridas manuales), `retries=0` porque
   *"reintentar no arregla datos malos"*, datos entre etapas como **archivos intermedios** y solo
   manifests en XCom.
2. **Cómo se ejecuta (tres caminos, mismo código):** `uv run python -m src.etl_main`,
   `podman-compose run --rm etl`, o la **UI de Airflow** en `localhost:8080` (sin credenciales).
3. **Demo A — corrida feliz (60 s):** trigger del DAG y ver las 6 tareas en verde.

```bash
podman exec airflow_api_server airflow dags trigger health_reliable_pipeline
```

4. **Demo B — fallo controlado (90 s, el corazón de Activity 3):**

```bash
podman exec airflow_api_server airflow dags trigger health_reliable_pipeline \
  --conf '{"affiliates_file": "affiliates_bad.csv"}'
```

Resaltar: `validate_raw_gate` **failed** (`QualityGateError`) → `transform`,
`validate_prepared_gate`, `load_warehouse` y `build_r1_r5_summary` quedan **upstream_failed**.
*"La carga nunca corrió: la puerta de calidad manda sobre el orquestador."*

5. **Ingeniería:** 104 pruebas (`uv run pytest`), imágenes Docker/Podman reproducibles, entorno
   Nix con `nix develop`, README/README_ES con el informe completo.
6. **Limitaciones** (foto temporal, nivel de atención 61 % nulo, afiliación ≠ acceso) y cierre.

**Frase de cierre (la dicen los 4):**
*"Cargamos datos, pero sobre todo los puertamos: si no pasa la calidad, no entra al warehouse."*

---

### Preguntas probables del jurado

| Pregunta | Respuesta corta |
|---|---|
| ¿Por qué dos puertas y no una? | Son momentos distintos: validar el **crudo** evita transformar basura; validar lo **preparado** evita cargar inconsistencias (FK, unicidad, rangos). |
| ¿Por qué Great Expectations y no `assert`? | Suites versionables, evidencia JSON por corrida, severidades y reportes; las mismas reglas corren en pruebas, CLI y Airflow. |
| ¿Por qué `retries=0`? | Los fallos son **de datos**, no transitorios: reintentar repite el mismo error y oculta el problema. |
| ¿Qué pasa si la puerta dice `STOP`? | Falla la tarea → el DAG Run queda `failed` y `load`/`summary` quedan `upstream_failed`. |
| ¿Por qué dos tablas de hechos? | Granos distintos (afiliados vs capacidad); forzar un solo hecho mezclaría medidas. |
| ¿Cuánto tarda una corrida completa? | ~25–40 s (extract 1 s, gates ~4 s, transform ~3 s, load ~2 s). |
| ¿Cómo se reproduce en otra máquina? | `uv sync --all-groups` desde `uv.lock` (o `nix develop`) + `podman-compose up -d`. |
| ¿Podman o Docker? | El compose funciona con ambos; en Linux usamos **Podman** (sin daemon, raíz-less). |
| ¿Dónde está la evidencia de calidad? | `logs/gx/*.json` (por corrida) y `data/work/manifest_*.json` (por etapa). |

---

### Plan B si la demo en vivo falla

1. **Airflow caído:** ejecutar `uv run python -m src.etl_main` y mostrar la misma salida por CLI
   (mismas etapas y puertas).
2. **Sin tiempo:** usar los artefactos ya generados:
   `data/work/manifest_validate_raw.json` (fallo), `logs/gx/*_latest.json` (política),
   `data/output/r1…r5.csv` (resultados).
3. **UI lenta:** mostrar el log de la tarea en
   `logs/dag_id=health_reliable_pipeline/run_id=…/task_id=…/attempt=1.log`.
4. **Reiniciar todo:** `podman-compose down && podman-compose up -d postgres airflow-init && podman-compose up -d`.
