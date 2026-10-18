"""Dashboard explorador de los 7 requerimientos R1–R7 con tabs y filtros nativos.

Una pestaña (TAB) por requerimiento: descripción + SQL del requerimiento,
chart principal y tabla de detalle. Filtros nativos compartidos (régimen,
región, departamento, categoría de cobertura, nivel de alerta).
"""

from __future__ import annotations

from common import (
    SupersetClient, ensure_database, ensure_dataset, ensure_chart,
    build_filters, apply_dashboard, simple_metric, _spec, _big, _table,
)

W = "requirements"

REQ_TITLE = "ODS 3 · Requerimientos R1–R7 (Explorador con Filtros)"
REQ_SLUG = "ods3-requerimientos"

REQ_CSS = """
/* --- Banner de tabs --- */
.superset-dashboard-component-tab .dashboard-component-chart-holder .chart-slice {
    border-radius: 8px !important;
}
/* --- Tarjetas de charts en tabs --- */
.dashboard-component-chart-holder:has([id^="CHART-t"]) {
    border: 1px solid rgba(128, 128, 128, 0.20) !important;
    border-radius: 8px !important;
    box-sizing: border-box !important;
}
.dashboard-component-chart-holder .chart-slice,
.dashboard-component-chart-holder .dashboard-chart,
.dashboard-component-chart-holder .slice_container {
    border: none !important; box-shadow: none !important; border-radius: 0 !important;
}
/* --- SQL en los markdown de tab --- */
[class^="MARKDOWN-t"] .dashboard-markdown pre,
.dashboard-markdown pre {
    background: rgba(2, 132, 199, 0.06) !important;
    border: 1px solid rgba(2, 132, 199, 0.25) !important;
    border-radius: 6px !important;
    padding: 8px 10px !important;
    font-size: 12px !important;
}
"""

# ---------------------------------------------------------------- charts


def _agg_table(groupby: list[str], metrics: list, limit: int = 50) -> tuple:
    form = {"query_mode": "aggregate", "groupby": groupby, "metrics": metrics,
            "row_limit": limit, "order_desc": True, "include_search": True,
            "table_timestamp_format": "smart_date", "table_format": "flexible"}
    return ("table", form)


def _bar(x: str, metric, limit: int, fmt: str = ",.0f",
         groupby: list[str] | None = None, stacked: bool = False,
         show_value: bool = True, orientation: str = "vertical") -> list:
    form = {
        "x_axis": x, "metrics": [metric], "orientation": orientation,
        "show_value": show_value, "y_axis_format": fmt, "row_limit": limit,
        "color_scheme": "supersetColors", "stack": "Stack" if stacked else "None",
        "x_axis_sort": (metric["label"] if isinstance(metric, dict) else metric),
        "x_axis_sort_asc": False,
    }
    if groupby:
        form["groupby"] = groupby
    return [("echarts_timeseries_bar", form)]


M_AFF = lambda: simple_metric("numero_afiliados", "SUM", "Afiliados")
M_BEDS = lambda: simple_metric("camas_por_1000_hab", "MAX", "Camas / 1000 hab", "NUMERIC")

REQ_CHARTS: list[dict] = [
    # --- R1
    _spec(W, "v_affiliates_summary", "Req · R1 — Afiliados por Régimen",
          "R1: distribución de afiliados SGSSS por régimen de cotización.",
          _bar("regime", M_AFF(), 10, orientation="horizontal")),
    _spec(W, "v_affiliates_summary", "Req · R1 — Detalle por Régimen",
          "R1: totales de afiliados por régimen (tabla).",
          [_agg_table(["regime"], [M_AFF()], 20)]),

    # --- R2
    _spec(W, "v_affiliates_summary", "Req · R2 — Top 10 Departamentos",
          "R2: departamentos con mayor número de afiliados al SGSSS.",
          _bar("departamento", M_AFF(), 10)),
    _spec(W, "v_affiliates_summary", "Req · R2 — Detalle Top 10",
          "R2: totales de afiliados por departamento (top 10).",
          [_agg_table(["departamento"], [M_AFF()], 10)]),

    # --- R3
    _spec(W, "v_facility_summary", "Req · R3 — Establecimientos por Naturaleza",
          "R3: establecimientos (IPS) por tipo de naturaleza jurídica.",
          _bar("nature", simple_metric("capacity_amount", "COUNT", "Establecimientos"), 20)),
    _spec(W, "v_facility_summary", "Req · R3 — Detalle por Naturaleza",
          "R3: conteo de establecimientos por naturaleza (tabla).",
          [_agg_table(["nature"], [simple_metric("capacity_amount", "COUNT", "Establecimientos")], 20)]),

    # --- R4
    _spec(W, "v_r7_territorial_capacity", "Req · R4 — Camas por 1000 Habitantes",
          "R4: departamentos con más camas cada 1000 habitantes.",
          _bar("departamento", M_BEDS(), 10, ",.3f")),
    _spec(W, "v_r7_territorial_capacity", "Req · R4 — Detalle Camas/1000",
          "R4: camas por 1000 habitantes por departamento (top 10).",
          [_agg_table(["departamento"], [M_BEDS()], 10)]),

    # --- R5
    _spec(W, "v_affiliates_summary", "Req · R5 — Afiliados por Región y Régimen",
          "R5: afiliados por Región del país y régimen (apilado).",
          _bar("region", M_AFF(), 50, groupby=["regime"], stacked=True, show_value=False)),
    _spec(W, "v_affiliates_summary", "Req · R5 — Detalle Región × Régimen",
          "R5: totales de afiliados por región y régimen (tabla).",
          [_agg_table(["region", "regime"], [M_AFF()], 50)]),

    # --- R6
    _spec(W, "v_r6_demographic_coverage", "Req · R6 — Cobertura Demográfica",
          "R6: cobertura de afiliación vs población oficial por departamento.",
          [_table(["departamento", "region", "poblacion_oficial_api",
                   "total_afiliados_sgsss", "cobertura_afiliacion_pct",
                   "brecha_poblacional", "categoria_cobertura"], 50)]),

    # --- R7
    _spec(W, "v_r7_territorial_capacity", "Req · R7 — Capacidad Territorial",
          "R7: capacidad instalada, camas/1000 hab y nivel de alerta por departamento.",
          [_table(["departamento", "region", "total_ips_habilitadas",
                   "total_camas", "capacidad_total_instalada",
                   "camas_por_1000_hab", "capacidad_por_1000_km2",
                   "nivel_alerta_hospitalaria"], 50)]),
]

# ------------------------------------------------- tabs (una pestaña por R)

REQ_MARKDOWN: dict[str, str] = {
    "tab_r1": (
        "### R1 · Afiliados por Régimen\n"
        "Distribución de los afiliados al SGSSS según el régimen de cotización "
        "(Contributivo, Subsidiado, Especial, Individuales).\n\n"
        "```sql\n"
        "SELECT regime, SUM(numero_afiliados) AS afiliados\n"
        "FROM v_affiliates_summary GROUP BY regime ORDER BY 2 DESC;\n"
        "```"
    ),
    "tab_r2": (
        "### R2 · Top 10 Departamentos por Afiliados\n"
        "Departamentos con mayor volumen de afiliados al SGSSS (concentración territorial "
        "de la cobertura).\n\n"
        "```sql\n"
        "SELECT departamento, SUM(numero_afiliados) AS afiliados\n"
        "FROM v_affiliates_summary GROUP BY departamento\n"
        "ORDER BY 2 DESC LIMIT 10;\n"
        "```"
    ),
    "tab_r3": (
        "### R3 · Establecimientos por Naturaleza\n"
        "Distribución de los establecimientos (IPS habilitados) por tipo de naturaleza "
        "jurídica (Pública, Privada, Mixta...).\n\n"
        "```sql\n"
        "SELECT nature, COUNT(capacity_amount) AS establecimientos\n"
        "FROM v_facility_summary GROUP BY nature ORDER BY 2 DESC;\n"
        "```"
    ),
    "tab_r4": (
        "### R4 · Camas por 1000 Habitantes\n"
        "Top departamentos por densidad de camas hospitalarias cada 1000 habitantes "
        "(capacidad de respuesta del sistema).\n\n"
        "```sql\n"
        "SELECT departamento, MAX(camas_por_1000_hab) AS camas_por_1000\n"
        "FROM v_r7_territorial_capacity\n"
        "GROUP BY departamento ORDER BY 2 DESC LIMIT 10;\n"
        "```"
    ),
    "tab_r5": (
        "### R5 · Afiliados por Región y Régimen\n"
        "Comparación de la afiliación entre Regiones del país y régimen "
        "(Contributivo vs Subsidiado) para evidenciar disparidades territoriales.\n\n"
        "```sql\n"
        "SELECT region, regime, SUM(numero_afiliados) AS afiliados\n"
        "FROM v_affiliates_summary GROUP BY region, regime\n"
        "ORDER BY region, 3 DESC;\n"
        "```"
    ),
    "tab_r6": (
        "### R6 · Cobertura Demográfica por Departamento\n"
        "Cobertura de afiliación SGSSS vs población oficial API Colombia: brecha "
        "poblacional y categoría de cobertura por departamento.\n\n"
        "```sql\n"
        "SELECT * FROM v_r6_demographic_coverage\n"
        "ORDER BY cobertura_afiliacion_pct ASC;\n"
        "```"
    ),
    "tab_r7": (
        "### R7 · Capacidad Territorial por Departamento\n"
        "Capacidad instalada (IPS, camas, salas), camas/1000 hab, densidad territorial "
        "e indicador de alerta hospitalaria por departamento.\n\n"
        "```sql\n"
        "SELECT * FROM v_r7_territorial_capacity\n"
        "ORDER BY camas_por_1000_hab ASC;\n"
        "```"
    ),
}

REQ_TABS: list[tuple[str, list]] = [
    ("R1 · Afiliados por Régimen", [
        [("md", "tab_r1", 12, 26)],
        [("chart", "Req · R1 — Afiliados por Régimen", 6, 46),
         ("chart", "Req · R1 — Detalle por Régimen", 6, 46)],
    ]),
    ("R2 · Top 10 Departamentos", [
        [("md", "tab_r2", 12, 26)],
        [("chart", "Req · R2 — Top 10 Departamentos", 7, 44),
         ("chart", "Req · R2 — Detalle Top 10", 5, 44)],
    ]),
    ("R3 · Establecimientos por Naturaleza", [
        [("md", "tab_r3", 12, 26)],
        [("chart", "Req · R3 — Establecimientos por Naturaleza", 6, 44),
         ("chart", "Req · R3 — Detalle por Naturaleza", 6, 44)],
    ]),
    ("R4 · Camas por 1000 Habitantes", [
        [("md", "tab_r4", 12, 26)],
        [("chart", "Req · R4 — Camas por 1000 Habitantes", 7, 44),
         ("chart", "Req · R4 — Detalle Camas/1000", 5, 44)],
    ]),
    ("R5 · Afiliados por Región y Régimen", [
        [("md", "tab_r5", 12, 26)],
        [("chart", "Req · R5 — Afiliados por Región y Régimen", 12, 46)],
        [("chart", "Req · R5 — Detalle Región × Régimen", 12, 44)],
    ]),
    ("R6 · Cobertura Demográfica", [
        [("md", "tab_r6", 12, 26)],
        [("chart", "Req · R6 — Cobertura Demográfica", 12, 50)],
    ]),
    ("R7 · Capacidad Territorial", [
        [("md", "tab_r7", 12, 26)],
        [("chart", "Req · R7 — Capacidad Territorial", 12, 50)],
    ]),
]

REQ_FILTERS: list[dict] = [
    {"name": "Régimen", "column": "regime",
     "description": "Régimen de cotización (Contributivo, Subsidiado, Especial, Individual)"},
    {"name": "Región", "column": "region", "description": "Región del país (Caribbean, Andean, Pacific...)"},
    {"name": "Departamento", "column": "departamento", "description": "Departamento DANE"},
    {"name": "Categoría de Cobertura", "column": "categoria_cobertura",
     "description": "R6: Déficit de Cobertura, Meta Universal, Alta Concentración"},
    {"name": "Nivel de Alerta Hospitalaria", "column": "nivel_alerta_hospitalaria",
     "description": "R7: Déficit Crítico, Moderada, Alta"},
]


def build_tabs_layout(title: str, tabs: list[tuple[str, list]],
                      chart_ids: dict[str, int], markdown: dict[str, str]) -> dict:
    layout: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["TABS_ID"],
                    "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER",
                      "meta": {"text": title}},
    }
    tab_ids = []
    for ti, (tab_title, rows) in enumerate(tabs):
        tab_id = f"TAB-t{ti}"
        tab_ids.append(tab_id)
        layout[tab_id] = {"type": "TAB", "id": tab_id, "children": [],
                          "parents": ["ROOT_ID", "GRID_ID", "TABS_ID"],
                          "meta": {"text": tab_title,
                                   "background": "BACKGROUND_TRANSPARENT"}}
        for ri, row in enumerate(rows):
            rid = f"ROW-t{ti}r{ri}"
            layout[rid] = {"type": "ROW", "id": rid, "children": [],
                           "parents": ["ROOT_ID", "GRID_ID", "TABS_ID", tab_id],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout[tab_id]["children"].append(rid)
            for mi, (kind, ref, width, height) in enumerate(row):
                cid = f"CHART-t{ti}r{ri}c{mi}"
                parents = ["ROOT_ID", "GRID_ID", "TABS_ID", tab_id, rid]
                if kind == "chart":
                    meta = {"width": width, "height": height,
                            "chartId": chart_ids[ref], "sliceName": ref}
                    ctype = "CHART"
                else:
                    meta = {"width": width, "height": height,
                            "code": markdown[ref]}
                    ctype = "MARKDOWN"
                layout[cid] = {"type": ctype, "id": cid, "children": [],
                               "parents": parents, "meta": meta}
                layout[rid]["children"].append(cid)
    layout["TABS_ID"] = {"type": "TABS", "id": "TABS_ID", "children": tab_ids,
                         "parents": ["ROOT_ID", "GRID_ID"]}
    return layout


def build(client: SupersetClient) -> int:
    database_id = ensure_database(client)

    used: dict[str, int] = {}
    cols: dict[int, set[str]] = {}
    for view in ("v_affiliates_summary", "v_facility_summary",
                 "v_r6_demographic_coverage", "v_r7_territorial_capacity"):
        ds_id, columns = ensure_dataset(client, database_id, view)
        used[view] = ds_id
        cols[ds_id] = columns

    dash = client.ensure("/api/v1/dashboard/", "slug", REQ_SLUG, {
        "dashboard_title": REQ_TITLE, "slug": REQ_SLUG,
        "published": True, "json_metadata": "{}",
    })
    dash_id = int(dash["id"])

    chart_ids: dict[str, int] = {}
    chart_dataset: dict[int, int] = {}
    dash_chart_ids: list[int] = []
    for spec in REQ_CHARTS:
        cid = ensure_chart(client, spec, used[spec["dataset"]], dash_id)
        chart_ids[spec["slice_name"]] = cid
        chart_dataset[cid] = used[spec["dataset"]]
        dash_chart_ids.append(cid)

    layout = build_tabs_layout(REQ_TITLE, REQ_TABS, chart_ids, REQ_MARKDOWN)
    filters = build_filters(chart_dataset, cols, REQ_FILTERS, dash_chart_ids)
    apply_dashboard(client, dash_id, REQ_TITLE, REQ_SLUG,
                    layout, filters, REQ_CSS)
    return dash_id


def main() -> None:
    client = SupersetClient()
    client.login()
    dash_id = build(client)
    print(f"[dashboard] '{REQ_TITLE}' listo (id={dash_id}): "
          f"{client.base_url}/superset/dashboard/{REQ_SLUG}/")


if __name__ == "__main__":
    main()
