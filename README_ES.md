# ETL Health Colombia — ODS 3: Salud y Bienestar

**Curso:** ETL (G01) — Ingeniería de Datos e Inteligencia Artificial
**Fase:** 1 — Desde Requisitos Analíticos hasta un Almacén de Datos Dimensional
**ODS:** Objetivo 3 — Salud y Bienestar | Meta 3.8: Lograr cobertura sanitaria universal

---

## 1. Objetivo del Proyecto

### 1.1 Objetivo General

Diseñar e implementar un **pipeline ETL** que transforme datos de salud crudos de fuentes abiertas colombianas en un **almacén de datos dimensional** (esquema estrella) para analizar disparidades geográficas en el acceso a la salud, alineado con la Meta 3.8 del ODS.

### 1.2 Objetivos Específicos

| # | Objetivo | Entregable |
|---|----------|------------|
| O1 | Perfilar y limpiar datasets crudos (afiliados + establecimientos) | Archivos CSV limpios con correcciones de calidad |
| O2 | Diseñar esquema estrella normalizado (5 dimensiones, 2 tablas de hechos) | Script DDL `sql/init.sql` |
| O3 | Construir pipeline ETL automatizado (Extract → Transform → Validate → Load) | Módulos Python `src/` |
| O4 | Enriquecer datos geográficos con API Colombia (capital, superficie, población) | `dim_geografia` con datos externos |
| O5 | Cargar datos en PostgreSQL y exportar CSVs para herramientas BI | DW PostgreSQL + CSVs en `data/processed/` |
| O6 | Responder 5 requisitos analíticos (R1–R5) con consultas SQL | `sql/analytical_queries.sql` |
| O7 | Visualizar resultados en dashboard Power BI | `visualizations/dashboard.png` |

### 1.3 Granularidad del Proyecto

| Aspecto | Alcance |
|---------|---------|
| **Geográfico** | Nacional — 33 departamentos + Bogotá D.C., 1,046+ municipios |
| **Temporal** | Snapshots transversales: Q2 2022 (afiliados), Q4 2022 (establecimientos) |
| **Institucional** | Establecimientos de salud (IPS) registrados en REPS |
| **Régimen** | Subsidiado, Contributivo, Especial, Individual |
| **Infraestructura** | Camas (CAMAS), salas de procedimientos (SALAS), equipos por nivel de atención |

### 1.4 Resultados Esperados

- **5 tablas de dimensión**: `dim_time`, `dim_geografia`, `dim_regime`, `dim_facility`, `dim_capacity_type`
- **2 tablas de hechos**: `fact_affiliates` (3,369 filas), `fact_facility_capacity` (31,496 filas)
- **Dimensión geográfica unificada** enriquecida con datos de API Colombia (capital, superficie, población)
- **Dashboard Power BI** con mapas geográficos, KPIs y gráficos de distribución por régimen
- **5 consultas analíticas** respondiendo los requisitos R1–R5

---

## 2. Definición del Problema Colombiano

El sistema de salud de Colombia enfrenta significativas disparidades territoriales en el acceso a servicios. Si bien el país ha alcanzado altas tasas de afiliación a través del Sistema General de Seguridad Social en Salud (SGSSS), la distribución de la infraestructura de salud (camas, equipos, servicios especializados) no se corresponde proporcionalmente con las necesidades de la población en los departamentos y municipios. Algunas regiones mantienen alta capacidad por afiliado mientras que otras enfrentan déficits críticos.

Este análisis se centra en dos dimensiones complementarias del acceso a la salud:

1. **Cobertura de afiliación**: Cuántos afiliados están inscritos por régimen (Subsidiado/Contributivo) en los municipios y departamentos.
2. **Capacidad de infraestructura**: Cuánta capacidad instalada (camas, salas de procedimientos, equipos) existe por establecimiento de salud.

Comprender la relación entre la densidad de afiliación y la capacidad de infraestructura es esencial para identificar regiones desatendidas y fundamentar decisiones de asignación de recursos.

**Alcance geográfico:** Nacional (33 departamentos + Bogotá D.C.), en todos los municipios.

**Población de interés:** Afiliados del sistema de salud colombiano y establecimientos de salud (IPS).

**Partidos interesados potenciales:** Ministerio de Salud y Protección Social (Minsalud), secretarías departamentales de salud, EPS (entidades promotoras de salud), gerentes de hospitales, investigadores de políticas de salud.

---

## 3. Objetivo Analítico y Requisitos (R1–R5)

**Objetivo Analítico:** Analizar la distribución de afiliados a la salud y la capacidad de infraestructura en los territorios colombianos para identificar disparidades geográficas y respaldar decisiones de asignación de recursos alineadas con la Meta 3.8 del ODS.

### Matriz de Requisitos

| ID | Requisito Analítico | Pregunta de Negocio | Decisión / Conocimiento Respaldado |
|----|----------------------|-------------------|------------------------------|
| R1 | Determinar el número total de afiliados por tipo de régimen a nivel nacional | ¿Cuántos colombianos están afiliados a cada régimen (Subsidiado, Contributivo, Especial, Individual)? | Comprender la composición del sistema de salud y el peso relativo de cada régimen para la planificación de políticas. |
| R2 | Identificar los departamentos con mayor y menor densidad de afiliados | ¿Qué departamentos concentran más/menos afiliados, y cómo se relaciona esto con su población? | Detectar disparidades territoriales en la cobertura de salud para priorizar intervenciones en departamentos desatendidos. |
| R3 | Analizar la distribución de establecimientos de salud por naturaleza (público/privado) y nivel de atención | ¿Cuál es la mezcla público-privada de los proveedores de salud y en qué niveles de atención operan? | Evaluar si el sector privado complementa o duplica la infraestructura pública, informando decisiones de inversión pública. |
| R4 | Comparar la capacidad instalada (camas, salas) por afiliado en los departamentos | ¿Qué departamentos tienen infraestructura suficiente en relación con su población afiliada? | Identificar regiones donde se necesita expansión de infraestructura para satisfacer la demanda. |
| R5 | Evaluar la relación entre el tipo de régimen y la distribución geográfica | ¿Los afiliados al régimen Subsidiado están concentrados en territorios diferentes a los afiliados al régimen Contributivo? | Comprender si la segmentación por régimen de salud se corresponde con la inequidad geográfica, respaldando el diseño de políticas focalizadas. |

---

## 4. Alineación con los ODS

**ODS 3 — Salud y Bienestar**
**Meta 3.8:** Lograr la cobertura sanitaria universal (CSU), incluyendo la protección contra el riesgo financiero, el acceso a servicios esenciales de salud de calidad y el acceso a medicamentos y vacunas esenciales seguros, eficaces, de calidad y asequibles para todos.

Este proyecto aborda directamente la Meta 3.8 mediante el análisis de:

- **Amplitud de cobertura** (R1, R2): Midiendo cuántos colombianos están afiliados al sistema de salud y dónde se encuentran.
- **Disponibilidad de servicios** (R3, R4): Evaluando si existe infraestructura de salud para atender a la población afiliada.
- **Equidad** (R5): Examinando si el acceso a los servicios de salud se distribuye equitativamente entre regímenes y territorios.

El SGSSS de Colombia alcanza ~99% de afiliación a nivel nacional, pero la afiliación no garantiza el acceso. Las brechas de infraestructura, las barreras geográficas y las diferencias de servicios por régimen crean disparidades efectivas en el acceso a la salud. Este análisis proporciona la base de datos para identificar y cuantificar estas brechas.

---

## 4. Selección de las Fuentes de Datos

| Característica | Dataset de Afiliados | Dataset de Establecimientos |
|---|---|---|
| **Fuente** | SISPRO — Sistema Integrado de Información de la Protección Social | REPS — Registro Especial de Prestadores de Servicios de Salud |
| **Institución / Propietario** | Ministerio de Salud y Protección Social (Minsalud) | Ministerio de Salud y Protección Social (Minsalud) |
| **URL** | https://www.datos.gov.co/Salud-y-Protecci-n-Social/Relaci-n-de-IPS-p-blicas-y-privadas-seg-n-el-nivel/s2ru-bqt6/about_data  | https://www.datos.gov.co/Salud-y-Protecci-n-Social/N-mero-de-afiliados-por-departamento-municipio-y-r/hn4i-593p/about_data  |
| **Mecanismo** | Descarga CSV de Datos Abiertos Colombia | Descarga CSV de Datos Abiertos Colombia |
| **Formato** | CSV (separado por comas, punto como separador de miles) | CSV (separado por comas) |
| **Registros** | 3,369 filas | 41,427 filas |
| **Atributos** | 8 columnas | 20 columnas |
| **Cobertura Geográfica** | 34 departamentos, 1,046 municipios | 38 departamentos, todos los municipios principales |
| **Cobertura Temporal** | Abril 2022 (instantánea del Q2 2022) | Noviembre 2022 (instantánea del Q4 2022, corte REPS) |
| **Medidas Numéricas** | `NumPersonas` (número de afiliados por departamento/municipio/régimen/mes) | `num cantidad capacidad instalada` (capacidad instalada: camas, salas de procedimientos, equipos) |
| **Atributos Categóricos** | Departamento, Municipio, Tipo de régimen (S/C/E/I) | Departamento, Municipio, Naturaleza (Pública/Privada/Mixta), Nivel de Atención, Grupo de Capacidad, Descripción de Capacidad |

### Matriz de Idoneidad del Dataset

| Criterio | Evaluación |
|---|---|
| **Institución / Propietario de Datos** | Ministerio de Salud y Protección Social (Minsalud) |
| **Fuente** | SISPRO (afiliados) / REPS (establecimientos) |
| **URL** | datos.gov.co — Portal oficial de datos abiertos |
| **Mecanismo** | Descarga CSV directa |
| **Formato** | CSV |
| **Número de Registros** | 3,369 (afiliados) + 41,427 (establecimientos) = 44,796 total |
| **Número de Atributos** | 8 (afiliados) + 20 (establecimientos) |
| **Cobertura Geográfica** | Nacional — los 33 departamentos + Bogotá D.C. |
| **Cobertura Temporal** | Q2 2022 (afiliados), Q4 2022 (establecimientos) — instantáneas transversales |
| **Medidas Numéricas Relevantes** | Número de afiliados, capacidad instalada (camas, salas, equipos) |
| **Atributos Categóricos Relevantes** | Tipo de régimen, departamento, municipio, naturaleza del establecimiento, nivel de atención, tipo de capacidad |
| **Problemas Potenciales de Calidad de Datos** | Puntos como separadores de miles en NumPersonas; 61% de care_level nulos en establecimientos; algunos datos de contacto faltantes; filas duplicadas de establecimientos para diferentes tipos de capacidad |
| **Relación con los Requisitos** | R1→NumPersonas por régimen; R2→NumPersonas por departamento; R3→naturaleza/care_level; R4→capacidad vs afiliados; R5→régimen × geografía |
| **Idoneidad para Modelado Dimensional** | Alta — grano claro (fila de afiliación por dept/muni/régimen/mes; fila de establecimiento por proveedor/tipo de capacidad), dimensiones naturales (tiempo, geografía, régimen, establecimiento, tipo de capacidad) |
| **Unidad de Observación (Fuente)** | Afiliados: una fila = total de afiliados para un régimen en un municipio para un mes. Establecimientos: una fila = entrada de capacidad instalada para un establecimiento y tipo de capacidad. |

---

## 6. Perfilamiento de Datos y Evaluación de Calidad

### 5.1 Dataset de Afiliados

| Atributo | Tipo | Nulos | Valores Únicos | Notas |
|---|---|---|---|---|
| CodDepto | String | 0 | 34 | Código DANE del departamento |
| Departamento | String | 0 | 34 | Nombre del departamento |
| CodMunicipio | String | 0 | 1,046 | Código DANE del municipio |
| Municipio | String | 0 | 1,046 | Nombre del municipio |
| IDRegimen | String | 0 | 4 | S=Subsidiado, C=Contributivo, E=Especial, I=Individual |
| Año | String | 0 | 1 | 2022 |
| Mes | String | 0 | 1 | 4 (solo abril) |
| NumPersonas | String | 0 | — | Rango: 1–6,410,877. Suma: 51,182,238. Punto usado como separador de miles. |

**Problemas de Calidad:**
- NumPersonas almacenado como string con puntos como separadores de miles (ej., "2.194" = 2,194)
- Cobertura temporal limitada a un solo mes (abril 2022)
- No se detectaron valores nulos
- No hay filas completamente duplicadas

### 5.2 Dataset de Establecimientos

| Atributo | Tipo | Nulos | Valores Únicos | Notas |
|---|---|---|---|---|
| Departamento | String | 0 | 38 | Nombre del departamento |
| Municipio | String | 0 | — | Nombre del municipio |
| Código prestador | String | 0 | — | Identificador único del proveedor |
| Nombre prestador | String | 0 | — | Nombre del establecimiento |
| nit IPS | String | 0 | — | NIT con separadores de coma |
| naturaleza | String | 0 | 3 | Pública, Privada, Mixta |
| num nivel atencion | String | 25,266 (61.0%) | — | Nivel de atención (1–4) |
| nom grupo capacidad | String | 0 | — | CAMAS, SALAS, etc. |
| nom descripcion capacidad | String | 0 | — | TPR, Adultos, Pediátrica, etc. |
| num cantidad capacidad instalada | String | 0 | — | Rango: 1–650+ |
| Fecha Corte | String | 0 | 1 | 5 de noviembre de 2022 |
| Gerente | String | 225 (0.5%) | — | Nombre del gerente |
| Email | String | 65 (0.2%) | — | Correo electrónico de contacto |
| Teléfono | String | 1,309 (3.2%) | — | Teléfono de contacto |

**Problemas de Calidad:**
- 61% de valores nulos en `num nivel atencion` (nivel de atención) — no todos los establecimientos reportan esto
- El NIT contiene separadores de coma que requieren limpieza
- Los números de teléfono tienen formato inconsistente
- Cada establecimiento puede aparecer en múltiples filas (una por tipo de capacidad)
- Alguna información de contacto faltante (gerente, correo electrónico, teléfono)

---

## 7. Trazabilidad de Requisitos a Datos

| Requisito | Atributos Requeridos | Transformación Necesaria | KPI / Análisis Esperado |
|---|---|---|---|
| **R1** — Afiliados por régimen | `IDRegimen`, `NumPersonas` | Parsear NumPersonas (eliminar separadores), agregar por régimen | Total de afiliados por tipo de régimen (S, C, E, I) |
| **R2** — Densidad departamental | `Departamento`, `NumPersonas`, `CodDepto` | Agregar por departamento, normalizar por población o área | Departamentos con mayor/menor cantidad de afiliados |
| **R3** — Establecimientos por naturaleza/nivel | `naturaleza`, `num nivel atencion`, `Código prestador` | Desduplicar establecimientos, agregar por naturaleza y nivel de atención | Distribución pública vs privada por nivel de atención |
| **R4** — Capacidad por afiliado | `num cantidad capacidad instalada`, `NumPersonas`, `Departamento` | Unir afiliados y establecimientos vía departamento, calcular razón capacidad/afiliado | Razón capacidad-afiliado por departamento |
| **R5** — Régimen × Geografía | `IDRegimen`, `Departamento`, `Municipio`, `NumPersonas` | Pivotar régimen por geografía, calcular distribución de régimen por departamento | Concentración Subsidiado vs Contributivo por región |

---

## 8. Estrategia de Preparación de Datos

| Problema Detectado | Estrategia | Justificación |
|---|---|---|
| **NumPersonas como string con puntos** | Eliminar puntos separadores de miles, convertir a entero | Requerido para agregación numérica en tablas de hechos |
| **NIT con comas** | Eliminar comas de las cadenas NIT | Estandarizar formato de identificador para coincidencia de dimensiones |
| **Formato de números de teléfono** | Extraer solo dígitos numéricos usando regex | Normalizar formatos de teléfono inconsistentes para almacenamiento en dimensiones |
| **Normalización de nombres de departamento** | Mayúsculas, eliminar acentos (NFKD), mapear alias (ej., "VALLE" → "VALLE DEL CAUCA") | Asegurar nombres de departamento consistentes entre datasets para join |
| **Departamento "NO APLICA"** | Mapear a "SIN DEPARTAMENTO" con código DANE "00" (preserva 1,315,701 afiliados) | Los registros sin asignación geográfica se retienen para consistencia de sumas; código sintético "00" permite trazabilidad |
| **Cero afiliados** | Eliminar filas donde NumPersonas ≤ 0 | Los registros con conteo cero no contribuyen al valor analítico |
| **Nivel de atención faltante (61% nulo)** | Almacenar como NULL (entero nullable) | El nivel de atención no es crítico para el análisis de capacidad; NULL preserva integridad de datos sin introducir categorías artificiales |
| **Filas duplicadas de establecimientos** | Mantener combinaciones únicas (provider_code, municipality) | Cada establecimiento aparece una vez en dim_facility; los tipos de capacidad son entradas separadas |
| **Derivados: quarter, periodo_codigo** | Calcular trimestre a partir del mes, generar código "YYYY-QN" | Habilita agregación temporal a granularidad trimestral |
| **Derivado: region** | Mapear departamentos a 6 regiones oficiales (Amazonia, Andina, Caribe, Insular, Orinoquia, Pacifico) | Habilita análisis a nivel regional requerido por R2 y R5 |

---

## 8. Declaración de Grano

**Una fila en `fact_affiliates` representa** el número total de afiliados acumulados a la salud para un tipo de régimen específico (Subsidiado, Contributivo, Especial o Individual) en una geografía específica (municipio/departamento/región) durante un trimestre específico (Q2 2022).

**Una fila en `fact_facility_capacity` representa** la capacidad instalada (camas, salas de procedimientos o equipos) para un establecimiento de salud específico (IPS), un tipo de capacidad específico y una geografía específica en un momento dado (instantánea del Q4 2022).

---

## 10. Modelo de Datos Dimensional

### 9.1 Esquema Estrella

El modelo dimensional fue simplificado a un esquema estrella unificado de 5 dimensiones. Ambas tablas de hechos referencian una **única dimensión de geografía conformada** (`dim_geografia`), la cual está enriquecida con datos de API Colombia (capital, superficie, población, municipalities_count, phone_prefix, region_api).

![Star Schema](diagrams/starShema.png)

### 9.2 Justificación de Dimensiones y Hechos

| Tabla | Justificación |
|---|---|
| **dim_time** | El análisis temporal es requerido por R1, R2, R4, R5. La granularidad trimestral coincide con la instantánea de datos. Semestre y año permiten agregaciones temporales multinivel. |
| **dim_geografia** | **Dimensión de geografía conformada** utilizada por ambas tablas de hechos. Incluye códigos DANE, municipio, departamento, región y enriquecimiento de API Colombia (capital, superficie, población, municipalities_count, phone_prefix, region_api). Elimina tablas geográficas redundantes. |
| **dim_regime** | El tipo de régimen es la dimensión analítica central para R1, R5. Mapea los códigos S/C/E/I a nombres descriptivos. |
| **dim_facility** | El análisis a nivel de establecimiento es requerido por R3, R4. Contiene atributos del proveedor (naturaleza, nivel de atención, contacto) para segmentación descriptiva. Referencia `dim_geografia` mediante FK `sk_geografia`. |
| **dim_capacity_type** | Los tipos de capacidad (CAMAS/SALAS × TPR/Adultos/Pediátrica) son requeridos para el análisis de infraestructura R3, R4. |
| **fact_affiliates** | Almacena la medida `numero_afiliados` al grano: trimestre + geografía + régimen. Soporta R1, R2, R5. Referencia `dim_geografia` directamente. |
| **fact_facility_capacity** | Almacena la medida `capacity_amount` al grano: establecimiento + tipo de capacidad + geografía + instantánea temporal. Soporta R3, R4. Referencia `dim_geografia` directamente para análisis geográfico. |

### 9.3 ¿Por Qué Dos Tablas de Hechos?

Las dos tablas de hechos tienen **granularidades diferentes**:
- `fact_affiliates` está agregada en (trimestre, geografía, régimen) — una relación muchos a muchos entre territorio y régimen.
- `fact_facility_capacity` está en (establecimiento, tipo de capacidad, geografía, tiempo) — datos de infraestructura individuales a nivel de establecimiento.

Ambas referencian la misma dimensión conformada `dim_geografia` directamente, habilitando análisis geográfico entre hechos (ej., capacidad por afiliado por departamento) sin duplicar datos geográficos.

### 9.4 Cambios de Normalización (v3)

| Cambio | Antes (v2) | Después (v3) | Justificación |
|---|---|---|---|
| Dimensión de geografía | `dim_geografia` usada por afiliados; `dim_department`/`dim_municipality` usada por establecimientos; `dim_department_api` para enriquecimiento API | **Una sola `dim_geografia`** usada por ambas tablas de hechos, enriquecida con datos de API Colombia | Elimina redundancia geográfica; atributos API (capital, superficie, población) viven directamente en `dim_geografia` |
| Tablas eliminadas | `dim_department`, `dim_municipality`, `dim_department_api` existían como dimensiones separadas | **Eliminadas** — todos los datos geográficos unificados en `dim_geografia` | Reduce complejidad del esquema de 8 a 5 dimensiones; elimina auxiliares de jerarquía innecesarios |
| FK geografía en `dim_facility` | Referenciaba `dim_municipality` | Referencia `dim_geografia` mediante FK `sk_geografia` | Vínculo geográfico directo sin dimensión intermedia de municipio |
| Geografía en `fact_facility_capacity` | Tenía FK `sk_geografia` a través de ruta separada | FK `sk_geografia` directo a dimensión unificada | Análisis geográfico simplificado sin join a través de jerarquía de establecimiento |

---

## 11. Pipeline ETL

### 10.1 Arquitectura

![Star Schema](diagrams/diagramArchitecture.png)


### 10.2 Extracción (`src/extract.py`)
- Lee archivos CSV crudos con `pd.read_csv(dtype=str)` para preservar el formato original
- Renombra columnas de español a inglés para consistencia
- No se aplican transformaciones de negocio durante la extracción

### 10.3 Transformación (`src/transform.py`)

**Preparación de Datos:**
- Elimina puntos separadores de miles de `NumPersonas`, convierte a entero
- Normaliza nombres de departamento y municipio (eliminación de acentos, mayúsculas, mapeo de alias)
- Mapea departamentos a regiones usando `REGION_MAP` (después de normalización, asegurando mapeo correcto)
- Elimina registros con departamentos "NO APLICA" y cero afiliados
- Extrae dígitos numéricos de números de teléfono
- Elimina comas de valores NIT

**Transformación Dimensional:**
- Construye **5 tablas de dimensiones** con claves sustitutas (`sk_*`): `dim_time`, `dim_geografia`, `dim_regime`, `dim_facility`, `dim_capacity_type`
- Construye `dim_geografia` unificada a partir de ambos datasets de afiliados y establecimientos, enriquecida con datos de API Colombia (capital, superficie, población, municipalities_count, phone_prefix, region_api)
- `dim_facility` referencia `dim_geografia` mediante FK `sk_geografia`
- Agrega `fact_affiliates` a granularidad trimestral
- Mapea claves compuestas de establecimientos (provider_code + municipality) a claves sustitutas
- Ambas tablas de hechos referencian `dim_geografia` directamente para análisis geográfico

### 10.4 Validación (`src/validate.py`)

| Regla | Descripción |
|---|---|
| **Validación Raw** | Valida datos crudos antes de la transformación: columnas requeridas, sin NumPersonas negativos, códigos de régimen válidos (S/C/E/I) |
| **Integridad FK** | Todas las claves foráneas en tablas de hechos referencian claves sustitutas existentes en tablas de dimensiones |
| **Medidas Nulas** | Sin valores nulos en columnas de medidas (`numero_afiliados`, `capacity_amount`) |
| **Medidas Negativas** | Sin valores negativos en columnas de medidas |
| **Claves Duplicadas** | Sin claves compuestas duplicadas en tablas de hechos |
| **Consistencia de Sumas** | El total de `numero_afiliados` en la tabla de hechos coincide con el total fuente dentro de 1% de tolerancia |

### 10.5 Carga (`src/load.py`)
- Conecta a PostgreSQL usando `psycopg2`
- Ejecuta `sql/init.sql` para reiniciar esquema (DROP + CREATE idempotente)
- Carga dimensiones primero, luego tablas de hechos
- Usa `ON CONFLICT DO NOTHING` para idempotencia
- Reinicia secuencias seriales después de la carga (`setval`)
- Transacción única: compromete en éxito, revierte en fallo
- **Exportación CSV:** Después de la carga a PostgreSQL, exporta todas las tablas de dimensiones y hechos como archivos CSV individuales a `data/processed/` para ingestión alternativa por herramientas BI (ej., importación CSV directa en Power BI)

---

## 12. Validación de Requisitos a Modelo

| Requisito | Dimensión(es) | Medida(s) | KPI / Consulta Esperado | ¿Soportado? |
|---|---|---|---|---|
| **R1** — Afiliados por régimen | dim_regime, dim_time | numero_afiliados | Total de afiliados agrupados por descripción de régimen | Sí |
| **R2** — Densidad departamental | dim_geografia, dim_time | numero_afiliados | Departamentos con mayor/menor total de afiliados | Sí |
| **R3** — Establecimientos por naturaleza/nivel | dim_facility, dim_capacity_type | capacity_amount | Conteo de establecimientos por naturaleza; capacidad por nivel de atención | Sí |
| **R4** — Capacidad por afiliado | dim_facility, dim_geografia | capacity_amount, numero_afiliados | Razón capacity_amount / numero_afiliados por departamento | Sí |
| **R5** — Régimen × Geografía | dim_regime, dim_geografia | numero_afiliados | Distribución Subsidiado vs Contributivo a través de departamentos | Sí |


---

## 13. Consultas Analíticas y KPIs

Todas las consultas se ejecutan contra el Data Warehouse de PostgreSQL.

### R1 — Total de Afiliados por Tipo de Régimen

```sql
SELECT
    r.description AS regime,
    SUM(f.numero_afiliados) AS total_affiliates,
    ROUND(SUM(f.numero_afiliados) * 100.0 / SUM(SUM(f.numero_afiliados)) OVER(), 2) AS pct_share
FROM fact_affiliates f
JOIN dim_regime r ON f.sk_regime = r.sk_regime
GROUP BY r.description
ORDER BY total_affiliates DESC;
```

| Tablas DW | Métrica/KPI | Resultado Principal |
|---|---|---|
| fact_affiliates, dim_regime | Total de afiliados por régimen; porcentaje de participación | Los regímenes Subsidiado y Contributivo están casi equilibrados a nivel nacional (~48% vs ~47.5%), con los regímenes Especial e Individual representando <5%. La verdadera disparidad surge a nivel regional (ver R5). |

### R2 — Departamentos con Mayor y Menor Cantidad de Afiliados

```sql
SELECT department, total_affiliates, 'Top 10' AS category FROM (
    SELECT g.departamento AS department, SUM(f.numero_afiliados) AS total_affiliates
    FROM fact_affiliates f
    JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
    GROUP BY g.departamento
    ORDER BY total_affiliates DESC
    LIMIT 10
) top10
UNION ALL
SELECT department, total_affiliates, 'Bottom 10' AS category FROM (
    SELECT g.departamento AS department, SUM(f.numero_afiliados) AS total_affiliates
    FROM fact_affiliates f
    JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
    GROUP BY g.departamento
    ORDER BY total_affiliates ASC
    LIMIT 10
) bottom10
ORDER BY category DESC, total_affiliates DESC;
```

| Tablas DW | Métrica/KPI | Resultado Principal |
|---|---|---|
| fact_affiliates, dim_geografia | Departamentos con mayor y menor volumen de afiliados | Bogotá D.C., Antioquia y Valle del Cauca concentran las mayores poblaciones de afiliados. Departamentos más pequeños como Vaupés, Guainía y Vichada tienen menos afiliados, reflejando baja densidad poblacional. |

### R3 — Establecimientos por Naturaleza y Nivel de Atención

```sql
SELECT
    fc.nature,
    fc.care_level,
    COUNT(DISTINCT fc.sk_facility) AS facility_count,
    SUM(c.capacity_amount) AS total_capacity
FROM fact_facility_capacity c
JOIN dim_facility fc ON c.sk_facility = fc.sk_facility
GROUP BY fc.nature, fc.care_level
ORDER BY fc.nature, fc.care_level;
```

| Tablas DW | Métrica/KPI | Resultado Principal |
|---|---|---|
| fact_facility_capacity, dim_facility | Conteo de establecimientos y capacidad por naturaleza y nivel de atención | Los establecimientos públicos dominan en atención de alta complejidad (niveles 3–4), mientras que los establecimientos privados se concentran en servicios de menor complejidad (niveles 1–2). |

### R4 — Capacidad Instalada por Afiliado por Departamento

```sql
WITH capacity_by_dept AS (
    SELECT
        g.departamento AS department,
        SUM(c.capacity_amount) AS total_capacity
    FROM fact_facility_capacity c
    JOIN dim_geografia g ON c.sk_geografia = g.sk_geografia
    WHERE g.departamento NOT IN ('SIN DEPARTAMENTO')
    GROUP BY g.departamento
),
affiliates_by_dept AS (
    SELECT
        g.departamento AS department,
        SUM(f.numero_afiliados) AS total_affiliates
    FROM fact_affiliates f
    JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
    WHERE g.departamento NOT IN ('SIN DEPARTAMENTO')
    GROUP BY g.departamento
)
SELECT
    COALESCE(a.department, c.department) AS department,
    COALESCE(c.total_capacity, 0) AS total_capacity,
    COALESCE(a.total_affiliates, 0) AS total_affiliates,
    ROUND(COALESCE(c.total_capacity, 0)::numeric / NULLIF(COALESCE(a.total_affiliates, 0), 0), 4) AS capacity_per_affiliate
FROM affiliates_by_dept a
FULL OUTER JOIN capacity_by_dept c ON a.department = c.department
ORDER BY capacity_per_affiliate ASC;
```

| Tablas DW | Métrica/KPI | Resultado Principal |
|---|---|---|
| fact_affiliates, fact_facility_capacity, dim_geografia | Razón capacidad-afiliado por departamento | Los departamentos más pequeños muestran razones altamente variables; algunos tienen exceso de capacidad mientras que otros enfrentan déficits críticos. SIN DEPARTAMENTO (registros no geolocalizados) se excluye de este análisis. |

### R5 — Distribución de Régimen por Región Geográfica

```sql
SELECT
    g.region,
    r.description AS regime,
    SUM(f.numero_afiliados) AS total_affiliates,
    ROUND(SUM(f.numero_afiliados) * 100.0 / SUM(SUM(f.numero_afiliados)) OVER(PARTITION BY g.region), 2) AS pct_within_region
FROM fact_affiliates f
JOIN dim_geografia g ON f.sk_geografia = g.sk_geografia
JOIN dim_regime r ON f.sk_regime = r.sk_regime
GROUP BY g.region, r.description
ORDER BY g.region, total_affiliates DESC;
```

| Tablas DW | Métrica/KPI | Resultado Principal |
|---|---|---|
| fact_affiliates, dim_geografia, dim_regime | Distribución de afiliados por región y régimen; porcentaje dentro de cada región | La región Andina concentra la mayoría de afiliados. Pacifico muestra una fuerte mayoría Subsidiada (impulsada por Valle del Cauca, Chocó y Nariño). Amazonia tiene la concentración proporcional más alta de Subsidiado. |

---

## 14. Reglas de Validación ETL

| Categoría de Regla | Validación | Implementación |
|---|---|---|
| **Conteos de Filas** | Los conteos de filas fuente coinciden con conteos de registros transformados | Registrado en cada fase ETL; comparación de notebook |
| **Nulos Críticos** | Sin nulos en columnas PK/FK o medidas | `validate_measures()` verifica nulos en `numero_afiliados` y `capacity_amount` |
| **Unicidad** | Sin claves compuestas duplicadas en tablas de hechos | `validate_no_duplicates()` en (sk_time, sk_geografia, sk_regime) y (sk_time, sk_facility, sk_capacity_type) |
| **Integridad PK/FK** | Todas las FK de hechos referencian SKs de dimensiones existentes | `validate_foreign_keys()` verifica cada FK contra su dimensión |
| **Integridad Referencial** | Las claves sustitutas de dimensiones son secuenciales y no nulas | Secuencias seriales reiniciadas vía `setval()` después de la carga |
| **Reglas de Negocio** | NumPersonas > 0; códigos de régimen válidos (S, C, E, I) | Forzado durante transformación; validado en `validate_raw_affiliates()` |
| **Conciliación** | La suma de la medida de hechos coincide con el total fuente dentro de 1% | `validate_sum_consistency()` compara total de hechos vs. suma fuente |
| **Recuperación de Conteo de Filas** | Establecimientos únicos en tabla de hechos coinciden con establecimientos únicos fuente | `validate_row_count()` con `nunique('sk_facility')` vs `provider_code.nunique()` |

---

## 13.1 Resultados de Verificación de Calidad de Datos

Después de la ejecución completa del pipeline ETL, los siguientes resultados confirman la integridad de los datos:

### Resumen de Carga

| Tabla | Registros | Descripción |
|---|---|---|
| dim_time | 2 | Q2 2022 (afiliados) + Q4 2022 (establecimientos) |
| dim_geografia | 2,240 | Todos los municipios con códigos DANE, región y enriquecimiento de API Colombia (capital, superficie, población) |
| dim_regime | 4 | Subsidiado, Contributivo, Especial, Individual |
| dim_facility | 10,921 | Proveedores de salud (IPS) únicos con FK sk_geografia |
| dim_capacity_type | 63 | Combinaciones CAMAS/SALAS × TPR/Adultos/Pediátrica |
| fact_affiliates | 3,369 | Granularidad trimestral (geografía + régimen) |
| fact_facility_capacity | 31,496 | Filas de capacidad por establecimiento + tipo de capacidad + geografía (desduplicado) |

### Resultados de Validación

| Verificación | Resultado |
|---|---|
| Nulos FK (fact_affiliates) | 0 |
| Nulos FK (fact_facility_capacity) | 0 |
| Medidas negativas | 0 |
| Claves de hechos duplicadas | 0 |
| Consistencia de sumas (afiliados) | 51,182,238 = 51,182,238 (100% coincidencia) |
| Municipios sin mapear | 0 |
| Total de errores de validación | **0** |

### Correcciones de Calidad de Datos Aplicadas

| Problema | Causa Raíz | Corrección |
|---|---|---|
| 5,072 filas de establecimientos eliminadas silenciosamente | Distritos especiales de salud (Cali, Cartagena, Barranquilla, Santa Marta, Buenaventura) reportados como "departamento" en REPS | Mapeo `DISTRICT_TO_DEPT` en `config.py` redirige a departamentos reales |
| 80 municipios sin mapear | CAUCA faltante en `DEPT_DANE_CODES`; municipios solo de establecimientos no en dataset de afiliados | Se agregó CAUCA (código 19); `dim_geografia` unificada ahora fusiona ambos datasets |
| 13,970 claves de hechos duplicadas | Los datos crudos REPS tienen filas duplicadas por establecimiento + tipo de capacidad | `build_fact_facility_capacity` ahora agrega con `groupby().sum()` |
| 1,315,701 afiliados perdidos ("NO APLICA") | `DEPT_NORMALIZE['NO APLICA'] = None` causaba eliminación silenciosa | Mapear a "SIN DEPARTAMENTO" con código DANE "00"; `raw_total` calculado antes de `clean_affiliates()` |
| Cauca sin región | CAUCA ausente en `REGION_MAP` | Se agregó `'CAUCA': 'Pacifico'` |
| Falso positivo de validación | `len(df_facilities)` comparaba filas totales (41,427) contra establecimientos únicos (10,921) | Cambiado a `provider_code.nunique()` para comparación consistente |
| CASANARE con código DANE incorrecto | CASANARE tenía código 19 (duplicado con CAUCA) | Corregido a 85 |
| Hash no determinístico | Python `hash()` varía entre ejecuciones | Reemplazado con `hashlib.md5()` |
| 29 establecimientos de AMAZONAS/GUAVIARE eliminados | `DEPT_DANE_CODES` faltaban estos dos departamentos | Se agregaron `'AMAZONAS': '91'`, `'GUAVIARE': '95'` + nueva verificación `validate_department_consistency()` |
| Valle del Cauca (4.6M afiliados) mapeado a "Sin Region" | `region` calculado en `extract.py` antes de que `DEPT_NORMALIZE` corrigiera "VALLE" → "VALLE DEL CAUCA" | Se movió cálculo de `region` a `clean_affiliates()` después de normalización; se agregó `validate_department_region_consistency()` |

---

## 15. Inteligencia de Negocios

Un dashboard de Power BI se conecta al Data Warehouse de PostgreSQL y proporciona:

- **Análisis Geográfico:** Visualización de mapa a nivel de departamento que muestra densidad de afiliados y distribución de capacidad.
- **Análisis Temporal:** Tendencia trimestral de afiliados por tipo de régimen.
- **Análisis Comparativo:** Distribución de establecimientos públicos vs. privados a través de niveles de atención.
- **KPIs:** Total de afiliados a nivel nacional, capacidad instalada total, razón capacidad-afiliado.
- **Filtros:** Por departamento, región, tipo de régimen, naturaleza del establecimiento y período de tiempo.

**Opciones de Carga de Datos para Power BI:**
1. **Conexión PostgreSQL** (recomendada): Conexión directa al Data Warehouse usando el conector PostgreSQL. Ver "Conectar Power BI (VM Windows) a PostgreSQL (Host Linux)" más abajo para instrucciones de configuración de VM.
2. **Importación CSV**: Alternativa — cargar archivos CSV individuales de `data/processed/`. Cada CSV incluye claves sustitutas para unir tablas en Power BI.

**Archivos CSV disponibles en `data/processed/`:**

| Archivo | Registros | Contenido |
|------|---------|---------|
| `dim_time.csv` | 2 | Q2 2022 (afiliados) + Q4 2022 (establecimientos) |
| `dim_geografia.csv` | 2,240 | Municipios con códigos DANE, región y enriquecimiento de API Colombia |
| `dim_regime.csv` | 4 | Subsidiado, Contributivo, Especial, Individual |
| `dim_facility.csv` | 10,921 | Proveedores de salud (IPS) con atributos y FK sk_geografia |
| `dim_capacity_type.csv` | 63 | CAMAS/SALAS × TPR/Adultos/Pediátrica |
| `fact_affiliates.csv` | 3,369 | Conteos de afiliados trimestrales por geografía/régimen |
| `fact_capacity.csv` | 31,496 | Registros de capacidad por establecimiento, tipo y geografía |

> **Nota:** Las capturas de pantalla del dashboard se guardan en `visualizations/`. Los diagramas fuente están en `diagrams/`.

![Power BI Dashboard](visualizations/dashboard.png)

---

## 16. Interpretación Analítica

### Hallazgo 1: Concentración del Régimen Subsidiado en Regiones Periféricas

**¿Qué muestran los datos?** A nivel nacional, los regímenes Subsidiado (~48%) y Contributivo (~47.5%) están casi equilibrados. Sin embargo, la distribución geográfica revela disparidades marcadas: el régimen Subsidiado es proporcionalmente mucho mayor en las regiones Pacifico (Chocó, Valle del Cauca, Nariño) y Amazonia, mientras que el régimen Contributivo domina en Andina y Caribe. Esta concentración regional — en lugar de la mayoría nacional — es la característica definitoria del patrón de afiliación del sistema de salud colombiano.

**¿Qué requisito aborda?** R5 — Relación entre tipo de régimen y distribución geográfica.

**¿Por qué es relevante en el contexto colombiano?** El régimen Subsidiado cubre a la población de menores ingresos. Su concentración desproporcionada en regiones periféricas (Pacifico, Amazonia) sugiere que estas poblaciones dependen fuertemente de la salud financiada por el estado. Sin embargo, estas mismas regiones a menudo tienen menos establecimientos de salud, creando una brecha de acceso entre la afiliación y la prestación efectiva de servicios.

**¿Qué decisión o investigación adicional podría respaldar?** Inversión de infraestructura focalizada en departamentos con alta afiliación Subsidiada pero bajas razones capacidad-afiliado. Investigar si los afiliados Subsidiados en estas regiones enfrentan tiempos de espera más largos o distancias de viaje mayores.

### Hallazgo 2: Disparidad en la Razón Capacidad-Afiliado entre Departamentos

**¿Qué muestran los datos?** Existe una variación significativa en la razón capacidad-afiliado entre departamentos. Algunos departamentos (ej., Amazonas, Vichada) muestran alta capacidad por afiliado debido a baja densidad poblacional, mientras que departamentos urbanos (Bogotá, Antioquia) muestran razones más bajas a pesar de tener más infraestructura absoluta.

**¿Qué requisito aborda?** R4 — Comparar capacidad instalada por afiliado entre departamentos.

**¿Por qué es relevante en el contexto colombiano?** La infraestructura de salud de Colombia sigue la demanda poblacional, pero los departamentos rurales y remotos pueden mantener capacidad que excede la demanda local debido a programas de inversión gubernamental. Por el contrario, las áreas urbanas en rápido crecimiento pueden enfrentar déficits de infraestructura.

**¿Qué decisión o investigación adicional podría respaldar?** Orientar los planes de expansión de infraestructura de Minsalud identificando departamentos donde la capacidad es genuinamente insuficiente vs. donde las razones altas reflejan baja utilización. Respaldar decisiones sobre reubicación de establecimientos o unidades móviles de salud.

### Hallazgo 3: La Mezcla Público-Privada Varía por Complejidad de Atención

**¿Qué muestran los datos?** Los establecimientos de salud públicos dominan en niveles de atención más altos (3 y 4 — atención hospitalaria y especializada), mientras que los establecimientos privados son más prevalentes en niveles de atención más bajos (1 y 2 — atención básica e intermedia). Un pequeño número de establecimientos (15) reporta como "Mixta" (naturaleza mixta), representando una tercera categoría marginal.

**¿Qué requisito aborda?** R3 — Distribución de establecimientos de salud por naturaleza y nivel de atención.

**¿Por qué es relevante en el contexto colombiano?** El sistema de salud de Colombia depende de proveedores privados (IPS) contratados por EPS. Los datos revelan que la atención compleja y de alto costo sigue siendo predominantemente pública, mientras que el sector privado se enfoca en servicios de menor complejidad y mayor volumen. Esto tiene implicaciones para las vías de referencia y los tiempos de espera.

**¿Qué decisión o investigación adicional podría respaldar?** Informar estrategias de asociaciones público-privadas. Evaluar si incentivar la inversión privada en atención de mayor complejidad podría reducir la carga sobre los hospitales públicos. Evaluar brechas geográficas donde ni los establecimientos públicos ni privados proporcionan cobertura adecuada.

---

## 17. Arquitectura del Sistema

![System Architecture](diagrams/systemArchitecture.png)
---

## 17. Implementación del Data Warehouse

### Prerrequisitos

- Python 3.9+
- PostgreSQL 12+ (o Docker/Podman)
- Docker Desktop (Windows) / Docker Engine (Linux) o Podman

### Opción A: Configuración Local (Sin Docker)

```bash
# 1. Create database and user
psql -U postgres -c "CREATE DATABASE salud_colombia;"
psql -U postgres -c "CREATE USER etl_user WITH PASSWORD 'etl_password_2026';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE salud_colombia TO etl_user;"
psql -U postgres -d salud_colombia -c "GRANT USAGE, CREATE ON SCHEMA public TO etl_user;"

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Initialize schema
psql -U etl_user -d salud_colombia -f sql/init.sql

# 4. Run ETL
python -m src.etl_main
```

### Opción B: Docker (Linux)

```bash
# Build and run everything
docker compose down -v
docker compose build --no-cache
docker compose up -d
docker compose run --rm etl

# Check logs
docker compose logs etl

# Stop all services
docker compose down -v
```

### Opción C: Docker (Windows — PowerShell)

```powershell
# Build and run everything
docker compose down -v
docker compose build --no-cache
docker compose up -d
docker compose run --rm etl

# Check logs
docker compose logs etl

# Stop all services
docker compose down -v
```

### Opción D: Podman (Linux)

```bash
# Build and run everything
podman-compose down -v
podman-compose build --no-cache
podman-compose up -d
podman-compose run --rm etl

# Check logs
podman-compose logs etl

# Stop all services
podman-compose down -v
```

### Opción E: Podman (Windows — PowerShell)

```powershell
# Build and run everything
podman-compose down -v
podman-compose build --no-cache
podman-compose up -d
podman-compose run --rm etl

# Check logs
podman-compose logs etl

# Stop all services
podman-compose down -v
```

### Salida del Pipeline ETL

Cuando ejecute el pipeline ETL, debería ver la siguiente salida:

```
============================================================
  ETL Salud Colombia — Warehouse Dimensional
============================================================

PHASE 1: EXTRACTION
[EXTRACT] Extracting affiliates from: data/raw/affiliates_by_department_municipality_regime_20260906.csv
[EXTRACT] Records extracted: 3369
[EXTRACT] Extracting facilities from: data/raw/healthcare_facilities_by_level_capacity_20260906.csv
[EXTRACT] Records extracted: 41427

PHASE 1.5: RAW VALIDATION
[VALIDATE] Raw validation passed

PHASE 2: TRANSFORMATION
[TRANSFORM] Cleaning affiliates dataset...
[TRANSFORM] Records after cleaning: 3369
[TRANSFORM] Cleaning facilities dataset...
[TRANSFORM] Records after cleaning: 41427
[TRANSFORM] Building dimensions...
[TRANSFORM] Time dimension records: 2
[TRANSFORM] Geography dimension records: 2240 (enriched with API Colombia)
[TRANSFORM] Regime dimension records: 4
[TRANSFORM] Facility dimension records: 10921
[TRANSFORM] Capacity type dimension records: 63
[TRANSFORM] Building fact tables...
[TRANSFORM] Affiliates fact records (quarterly): 3369
[TRANSFORM] Facility capacity fact records: 31496

PHASE 2.5: VALIDATION
[VALIDATE] All validations passed successfully

PHASE 3: LOADING
[LOAD] Schema reset from init.sql
[LOAD] Loading dimension dim_time: 2 records
[LOAD] Loading dimension dim_geografia: 2240 records
[LOAD] Loading dimension dim_regime: 4 records
[LOAD] Loading dimension dim_facility: 10921 records
[LOAD] Loading dimension dim_capacity_type: 63 records
[LOAD] Loading fact table fact_affiliates: 3369 records
[LOAD] Loading fact table fact_facility_capacity: 31496 records
[LOAD] Load completed successfully

PHASE 4: CSV EXPORT
[EXPORT] Exported dim_time: 2 records
[EXPORT] Exported dim_geografia: 2240 records
[EXPORT] Exported dim_regime: 4 records
[EXPORT] Exported dim_facility: 10921 records
[EXPORT] Exported dim_capacity_type: 63 records
[EXPORT] Exported fact_affiliates: 3369 records
[EXPORT] Exported fact_capacity: 31496 records

============================================================
  ETL PROCESS COMPLETED SUCCESSFULLY
============================================================
```

### Variables de Entorno

| Variable | Predeterminada | Descripción |
|---|---|---|
| `DB_HOST` | localhost (local) / postgres (Docker) | Host de PostgreSQL |
| `DB_PORT` | 5432 | Puerto de PostgreSQL |
| `DB_NAME` | salud_colombia | Nombre de la base de datos |
| `DB_USER` | etl_user | Usuario de la base de datos |
| `DB_PASSWORD` | etl_password_2026 | Contraseña de la base de datos |

### Conexión con DBeaver

DBeaver es una herramienta de bases de datos universal y gratuita utilizada para visualizar y consultar el Data Warehouse.

1. **Descargar e instalar** DBeaver desde https://dbeaver.io/download
2. **Crear una nueva conexión:**
   - Abrir DBeaver → Hacer clic en "Nueva Conexión a Base de Datos" (ícono de enchufe)
   - Seleccionar **PostgreSQL** → Hacer clic en Siguiente
3. **Configurar la conexión:**

| Campo | Valor |
|---|---|
| Server Host | `localhost` (local) o `localhost` (Docker, ya que el puerto 5432 está mapeado) |
| Puerto | `5432` |
| Base de datos | `salud_colombia` |
| Usuario | `etl_user` |
| Contraseña | `etl_password_2026` |

4. **Probar la conexión** → Hacer clic en "Finalizar"
5. **Verificar el esquema:**
   - Expandir `salud_colombia` → Schemas → `public` → Tables
   - Debería ver: `dim_time`, `dim_geografia`, `dim_regime`, `dim_facility`, `dim_capacity_type`, `fact_affiliates`, `fact_facility_capacity`
6. **Ejecutar consultas analíticas:**
   - Abrir un editor SQL (hacer clic derecho en `salud_colombia` → SQL Editor → Nuevo SQL Editor)
   - Pegar consultas de `sql/analytical_queries.sql`
   - Ejecutar con Ctrl+Enter

> **Nota:** Si usa Docker, el puerto de PostgreSQL está expuesto en `5432` por defecto. Si ese puerto ya está en uso, cambie el mapeo en `docker-compose.yml` (ej., `"5433:5432"`) y actualice `DB_PORT` en su archivo `.env`.

### Conectar Power BI (VM Windows) a PostgreSQL (Host Linux)

Si está ejecutando Power BI Desktop en una máquina virtual Windows y PostgreSQL está ejecutándose en un host Linux (vía Podman/Docker), siga estos pasos para establecer la conexión.

#### Prerrequisitos

- Power BI Desktop instalado en la VM Windows
- PostgreSQL ejecutándose en el host Linux (vía Podman/Docker)
- Conectividad de red entre la VM y el host Linux

#### Paso 1: Encontrar la Dirección IP del Host Linux

En el host Linux, ejecutar:

```bash
ip addr show virbr0 2>/dev/null || ip addr show | grep "inet " | grep -v 127.0.0.1
```

La IP predeterminada de la red libvirt/virbr0 es típicamente `192.168.122.1`.

#### Paso 2: Configurar la Conexión en Power BI

1. Abrir **Power BI Desktop** en la VM Windows
2. Hacer clic en **Obtener datos** → **Más...**
3. Seleccionar **Base de datos PostgreSQL** y hacer clic en **Conectar**
4. Ingresar la configuración de conexión:

| Configuración | Valor |
|---------|-------|
| **Servidor** | `<LINUX_HOST_IP>:5432` (ej., `192.168.122.1:5432`) |
| **Base de datos** | `salud_colombia` |
| **Modo de conectividad de datos** | `Importación` (recomendado para desarrollo) o `DirectQuery` |

5. Hacer clic en **Aceptar**

#### Paso 3: Autenticar

En el diálogo de autenticación:

| Pestaña | Configuración | Valor |
|-----|---------|-------|
| **Base de datos** | Usuario | `etl_user` |
| **Base de datos** | Contraseña | `etl_password_2026` |

6. Hacer clic en **Conectar**

#### Paso 4: Manejar Advertencias de SSL/Certificados

Si Windows solicita instalar el conector Npgsql o reporta certificados SSL faltantes:

- Aceptar la advertencia para conexión sin cifrar en la red virtual interna
- Esto es seguro para entornos locales/desarrollo

#### Paso 5: Seleccionar Tablas

1. En el Navigator, expandir el esquema `public`
2. Seleccionar las tablas que desea importar:

| Tabla | Contenido |
|-------|---------|
| `dim_time` | Períodos de tiempo (Q2 2022, Q4 2022) |
| `dim_geografia` | Municipios con códigos DANE, región y enriquecimiento de API Colombia |
| `dim_regime` | Tipos de régimen (Subsidiado, Contributivo, Especial, Individual) |
| `dim_facility` | 10,921 establecimientos de salud (IPS) con FK sk_geografia |
| `dim_capacity_type` | 63 tipos de capacidad (CAMAS/SALAS × TPR/Adultos/Pediátrica) |
| `fact_affiliates` | 3,369 registros de afiliados (granularidad trimestral) |
| `fact_facility_capacity` | 31,496 registros de capacidad |

3. Hacer clic en **Cargar** o **Transformar datos** si necesita limpiar los datos primero

#### Resumen de Conexión

| Componente | Valor |
|-----------|-------|
| **IP del Servidor** | `192.168.122.1` (virbr0 predeterminado) o la IP de su host |
| **Puerto** | `5432` |
| **Base de datos** | `salud_colombia` |
| **Usuario** | `etl_user` |
| **Contraseña** | `etl_password_2026` |
| **Modo SSL** | `Preferir` o `Deshabilitar` (para VMs locales) |

> **Nota:** Si la conexión falla, asegúrese de que el contenedor de PostgreSQL esté vinculado a `0.0.0.0:5432` (no solo `127.0.0.1:5432`) para que acepte conexiones externas. El `docker-compose.yml` de este proyecto ya configura esto correctamente.

---

## 19. Limitaciones y Supuestos

1. **Limitación de instantánea temporal:** Ambos datasets son instantáneas transversales (Q2 2022 para afiliados, Q4 2022 para establecimientos). El análisis de tendencias temporales verdadero requeriría múltiples períodos.
2. **Brechas en nivel de atención:** El 61% de los registros de establecimientos carecen de un valor de nivel de atención, limitando la granularidad del análisis por nivel de atención. Los valores faltantes se almacenan como NULL en el data warehouse.
3. **Afiliación ≠ Acceso:** Las altas tasas de afiliación no necessarily garantizan acceso efectivo a la salud. El análisis mide la inscripción al sistema, no la utilización de servicios.
4. **Definición de capacidad:** `capacity_amount` representa capacidad instalada (camas, salas, equipos), no capacidad operativa o efectiva.
5. **Municipios de establecimientos:** Los establecimientos en municipios no presentes en el dataset de afiliados se recuperan mediante códigos DANE sintéticos (hashlib.md5) para preservar la completitud de datos REPS.
6. **Advertencia de comparación entre datasets:** R4 (capacidad por afiliado) usa datos de capacidad del Q4 2022 con datos de afiliación del Q2 2022. La razón es indicativa, no temporalmente precisa.

---

## 20. Tecnologías

| Componente | Tecnología |
|---|---|
| Programación | Python 3.11, Pandas, NumPy, Requests |
| Perfilamiento | Jupyter Notebook |
| Data Warehouse | PostgreSQL 12+ |
| Pipeline ETL | Python (extract.py → transform.py → validate.py → load.py) |
| Dashboard BI | Power BI |
| Control de Versiones | Git, GitHub |

### Opción F: Nix Flake (NixOS / Linux)

Si usa Nix con el `flake.nix` proporcionado, ingrese al shell de desarrollo:

```bash
nix develop
```

Esto configura automáticamente Python 3.12, PostgreSQL, Docker/Podman, Jupyter y todas las dependencias.

**Comandos disponibles dentro del shell Nix:**

| Comando | Descripción |
|---|---|
| `docker-compose up -d` | Iniciar infraestructura (Docker) |
| `podman-compose up -d` | Iniciar infraestructura (Podman) |
| `docker-compose run --rm etl` | Ejecutar pipeline ETL |
| `./run_tests.sh unit` | Ejecutar pruebas unitarias |
| `./run_tests.sh all` | Ejecutar todas las pruebas |
| `jupyter lab` | Iniciar JupyterLab |
| `jupyter notebook` | Iniciar Jupyter Notebook clásico |
| `uv pip install <pkg>` | Instalar un paquete |

---

## 20. Miembros del Equipo y Responsabilidades

| Miembro | Rol | Responsabilidades |
|---|---|---|
| Deyton Riascos Ortiz | Project Manager | Coordinar el proyecto, organizar tareas, consolidar el informe final y preparar la presentación |
| Samuel Izquierdo Bonilla | Development Team | Liderar la definición de requisitos, historias de usuario, KPIs y mapeo de datos |
| Daniel David Garcia Restrepo | Development Team | Priorizar requisitos, validar que las respuestas cumplan con las necesidades del cliente y revisar objetivos de negocio |
| Mauricio Taborda Gongora | Tester | Validar calidad de datos, verificar resultados ETL, realizar pruebas de integración y unitarias |

---

## 22. Estructura del Proyecto

```
etl-project-first-delivery/
├── data/
│   ├── raw/                              # Source CSV files (preserved)
│   └── processed/                        # ETL output: dimension & fact CSVs for Power BI
├── diagrams/                             # Mermaid source + exported PNGs (star_schema, architecture, dashboard)
├── visualizations/                       # Dashboard screenshots
├── notebooks/
│   └── 01_data_validation_cleanup.ipynb  # Jupyter profiling notebook
├── src/
│   ├── __init__.py
│   ├── config.py                         # DB config, mappings, normalizers
│   ├── extract.py                        # CSV ingestion
│   ├── transform.py                      # Cleaning + dimensional builders
│   ├── validate.py                       # Data quality checks
│   ├── load.py                           # PostgreSQL bulk load + CSV export
│   └── etl_main.py                       # Pipeline orchestrator
├── sql/
│   ├── init.sql                          # DW schema (DDL)
│   └── analytical_queries.sql            # R1–R5 analytical queries
├── docs/
│   └── informe.tex                       # Project report (LaTeX)
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 23. Reproducibilidad

### Inicio Rápido con Docker/Podman

```bash
git clone <repository-url>
cd etl-project-first-delivery

# Create .env file (or configure in docker-compose.yml)
cat > .env << EOF
POSTGRES_DB=salud_colombia
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_password_2026
POSTGRES_PORT=5432
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
EOF

# Build and run
docker compose build --no-cache && docker compose up -d && docker compose run --rm etl
# OR
podman-compose build --no-cache && podman-compose up -d && podman-compose run --rm etl

# After ETL completes, CSV files are in data/processed/
ls data/processed/
```

### Inicio Rápido sin Docker (Local)

```bash
git clone <repository-url>
cd etl-project-first-delivery
pip install -r requirements.txt
psql -U postgres -c "CREATE DATABASE salud_colombia;"
psql -U postgres -c "CREATE USER etl_user WITH PASSWORD 'etl_password_2026';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE salud_colombia TO etl_user;"
psql -U postgres -d salud_colombia -c "GRANT USAGE, CREATE ON SCHEMA public TO etl_user;"
psql -U etl_user -d salud_colombia -f sql/init.sql
python -m src.etl_main
```
