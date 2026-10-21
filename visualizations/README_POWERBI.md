# Dashboard Power BI — Salud Colombia (ODS 3)

Informe de Power BI Desktop que consume el Data Warehouse `salud_colombia` (PostgreSQL) del pipeline y responde los requerimientos analíticos **R1 a R7** sobre aseguramiento e infraestructura de salud por departamento.

> Archivo: `dashboard_salud_colombia.pbix` (ajustar al nombre real del archivo en el repositorio).

## 1. Requisitos

- **Power BI Desktop** (Windows). Se desarrolló con el visual *Tarjeta (nuevo)*; versiones antiguas pueden mostrar las tarjetas distinto.
- El Data Warehouse **levantado y con datos** (ver `README.md`, sección 15). Desde la raíz del repositorio:
  - Windows: `run.bat up` y luego `run.bat trigger` (o ejecutar el DAG en Airflow).
  - Verificar puertos con `run.bat ports`.
- Credenciales de PostgreSQL: son las variables `POSTGRES_USER` y `POSTGRES_PASSWORD` del archivo `.env`. **No se guardan en este repositorio.**

## 2. Abrir el informe

1. Levanta los servicios y confirma que PostgreSQL responde.
2. Abre el `.pbix`.
3. Si Power BI pide credenciales, elige **Base de datos** e ingresa el usuario y la contraseña del `.env`.
4. Clic en **Inicio → Actualizar** para traer los datos más recientes.

### Conexión usada

| Parámetro | Valor |
|---|---|
| Conector | PostgreSQL |
| Servidor | `localhost:5434` |
| Base de datos | `salud_colombia` |
| Esquema | `public` |

El puerto **5434** es el mapeado en la máquina de desarrollo (variable `POSTGRES_PORT` del `.env`). Si en tu entorno es otro (el valor por defecto de `docker-compose.yml` es 5432), cámbialo así:

**Inicio → Transformar datos → Configuración de origen de datos → Cambiar origen** y edita el servidor. Después **Cerrar y aplicar**.

> Alternativa documentada en el proyecto: conexión vía PostgREST (`http://localhost:3000`, conector Web). Ver `docs/powerbi_postgrest_guide.md`. Este informe usa conexión directa a PostgreSQL.

## 3. Modelo de datos

Esquema en estrella con las 7 tablas del esquema `public`. En Power Query se renombraron quitando el prefijo `public `.

| Tabla | Tipo | Contenido |
|---|---|---|
| `fact_affiliates` | Hecho | Afiliados por municipio y régimen (corte Q2 2022) |
| `fact_facility_capacity` | Hecho | Capacidad instalada por sede y tipo de capacidad (corte Q4 2022) |
| `dim_geografia` | Dimensión | Municipio, departamento, región (nivel municipal) |
| `dim_regime` | Dimensión | Régimen de afiliación |
| `dim_facility` | Dimensión | Sede prestadora: naturaleza y nivel de atención |
| `dim_capacity_type` | Dimensión | Tipo y grupo de capacidad (camas, consultorios, etc.) |
| `dim_time` | Dimensión | Periodos de corte |
| `dim_departamento` | Dimensión derivada | 1 fila por departamento (34 filas) con población y superficie |
| `Medidas` | Tabla de medidas | Solo contiene medidas DAX, sin datos |

### Relaciones (muchos a uno, filtro en un solo sentido)

- `fact_affiliates` → `dim_geografia`, `dim_regime`, `dim_time`
- `fact_facility_capacity` → `dim_geografia`, `dim_facility`, `dim_capacity_type`, `dim_time`
- `dim_geografia[codigo_dane_depto]` → `dim_departamento[codigo_dane_depto]`
- `dim_facility[sk_geografia]` → `dim_geografia`: **inactiva a propósito**, para evitar una ruta de filtro ambigua.

### Transformaciones hechas en Power Query

- **`dim_departamento`:** consulta por *Referencia* de `dim_geografia`; conserva `codigo_dane_depto`, `departamento`, `region`, `capital`, `surface`, `population`; quita duplicados por `codigo_dane_depto`. Es necesaria porque `population` y `surface` vienen repetidas en cada municipio y **no se pueden sumar desde `dim_geografia`**.
- **`dim_facility[care_level]`:** convertido a texto; los nulos pasan a `Sin nivel`.

## 4. Medidas DAX (tabla `Medidas`)

| Medida | Qué calcula |
|---|---|
| `Total Afiliados` | Suma de `numero_afiliados` |
| `Afiliados ubicados` | Afiliados con departamento asignado (excluye `SIN DEPARTAMENTO`) |
| `Población` | Suma de población departamental (desde `dim_departamento`) |
| `Superficie km2` | Suma de superficie departamental |
| `Cobertura %` | `Afiliados ubicados / Población` |
| `Brecha` | `Población − Afiliados ubicados` (negativa si hay más afiliados que población) |
| `Brecha positiva` | Suma de los déficits por departamento (ignora los que superan el 100 %) |
| `% Participación` | Peso de cada régimen sobre el total filtrado |
| `% Subsidiado`, `% Contributivo` | Peso de cada régimen sobre el total de afiliados |
| `Capacidad Total` | Suma de `capacity_amount` (mezcla unidades distintas: no usar como "camas") |
| `Sedes` | Conteo de sedes |
| `Camas` | Capacidad del grupo `CAMAS` |
| `Capacidad por 1000 afiliados` | `Capacidad Total / Total Afiliados × 1000` |
| `Camas por 1000 hab` | `Camas / Población × 1000` |
| `Capacidad por 1000 km2` | `Capacidad Total / Superficie km2 × 1000` |
| `Alerta OMS` | `Cumple` si camas por 1.000 hab. > 2,5; si no, `Bajo estándar` |

## 5. Páginas del informe

| Página | Requerimiento | Qué muestra |
|---|---|---|
| **Resumen** | R1, R2 | Afiliados, población, cobertura nacional; participación por régimen; Top 10 y Bottom 10 de departamentos |
| **Infraestructura** | R3 | Sedes y capacidad por naturaleza y nivel de atención; capacidad por tipo |
| **Cobertura** | R4, R6 | Brecha, cobertura por departamento, capacidad vs. cobertura |
| **Equidad** | R5 | Régimen subsidiado vs. contributivo por región y departamento |
| **Territorial** | R7 | Camas por 1.000 hab. vs. superficie y alerta frente al estándar de 2,5 |

Todas las páginas incluyen un segmentador de región.

## 6. Cómo leer los resultados (notas importantes)

- **Cortes distintos:** afiliados corresponden al periodo Q2 de 2022 y capacidad al Q4 de 2022. Por eso **no hay segmentador de tiempo**: filtrar por un periodo dejaría el otro hecho en cero.
- **Cobertura mayor al 100 %:** en algunos departamentos (por ejemplo Risaralda) el registro de afiliados supera la población proyectada. Es una característica del dato (registro administrativo vs. proyección DANE), no un error del modelo.
- **Afiliados sin departamento:** existen afiliados sin ubicación (`SIN DEPARTAMENTO`). Las páginas por departamento los excluyen con un filtro de página.
- **Nivel de atención:** solo está definido para sedes públicas; casi todas las privadas aparecen como `Sin nivel`.
- **`Capacidad Total`** suma unidades heterogéneas. Para camas usar la medida `Camas`.
- **Estándar de 2,5 camas por 1.000 habitantes:** referencia de la OMS usada para la alerta de R7.

## 7. Problemas frecuentes

| Síntoma | Causa probable | Solución |
|---|---|---|
| No conecta a PostgreSQL | Servicios apagados o puerto distinto | `run.bat up`; revisar `POSTGRES_PORT` y cambiar el origen de datos |
| Error de credenciales | Usuario o contraseña distintos del `.env` | Inicio → Transformar datos → Configuración de origen de datos → Editar permisos |
| Tablas vacías tras actualizar | El pipeline no se ha ejecutado | `run.bat trigger` y volver a actualizar |
| Población 5.000 millones | Se sumó `population` desde `dim_geografia` | Usar la medida `Población` (desde `dim_departamento`) |
| Cifras de una tabla en cero al filtrar | Segmentador de tiempo | Quitar el filtro por `dim_time` |
| Error de ruta ambigua en relaciones | Se activó `dim_facility → dim_geografia` | Dejarla inactiva |

## 8. Cómo modificar el informe sin romperlo

- Las tablas de `public` son un contrato: el proyecto solo agrega columnas o tablas, no renombra ni elimina las publicadas (ver `README.md`, sección 10.6). Si cambia el DW, actualiza el informe antes de publicar.
- Crea las medidas nuevas en la tabla `Medidas`, no en las tablas de hechos.
- No marques relaciones como bidireccionales.
- El `.pbix` es un archivo binario: no se compara en Git. Se recomienda versionarlo con Git LFS o guardar copias con fecha, y no incluir credenciales dentro del archivo.
