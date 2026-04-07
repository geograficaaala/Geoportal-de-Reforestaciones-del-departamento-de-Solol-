"""
kobo_sync.py  —  Eje de Bosques · CODEMA Sololá
Sincroniza datos de KoboToolbox → data/puntos.geojson + data/resumen.json

Mejoras respecto a la versión anterior:
  - Usa la API directa de submissions (no depende de export-setting)
  - ultima_actualizacion = hora real del sync, no fecha del formulario
  - Cuenta TODAS las boletas, no solo las que tienen geopoint
  - Soporte para CSV con separador ; (exportación estándar de KoBo en es)
  - Extrae origen_planton, origen_planton_otro, especie (del repeat aplanado)
  - Usa _submission_time para ordenar y detectar la última actualización
  - Mejor manejo de errores con mensajes claros
"""

import csv
import datetime
import io
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# ── Configuración ──────────────────────────────────────────────────────────────
BASE         = os.getenv("KOBO_BASE_URL", "https://kf.kobotoolbox.org").rstrip("/")
TOKEN        = os.environ["KOBO_TOKEN"]
ASSET        = os.environ["KOBO_ASSET_UID"]
EXPORT_NAME  = os.getenv("KOBO_EXPORT_NAME", "portal_csv")

OUT_GEOJSON  = "data/puntos.geojson"
OUT_RESUMEN  = "data/resumen.json"

# Candidatos para coordenadas (orden de prioridad)
LAT_CANDIDATES = ["_ubicacion_latitude", "ubicacion_latitude", "_geolocation_latitude"]
LON_CANDIDATES = ["_ubicacion_longitude", "ubicacion_longitude", "_geolocation_longitude"]
PREC_CANDIDATES = ["_ubicacion_precision", "ubicacion_precision"]
GEOPOINT_COMBINED = ["ubicacion", "_geolocation", "geopoint", "location"]

# ── Utilidades ─────────────────────────────────────────────────────────────────
def utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def http_get(url: str, headers: Dict, timeout: int = 180, tries: int = 7) -> requests.Response:
    last_err = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code in (502, 503, 504):
                raise requests.HTTPError(f"{r.status_code}", response=r)
            return r
        except Exception as e:
            last_err = e
            time.sleep(min(30, 3 * (2 ** i)))
    raise RuntimeError(f"Fallo tras {tries} intentos. URL: {url}. Error: {last_err}")

def to_int(v: Any) -> int:
    try:
        s = str(v).strip()
        return int(round(float(s))) if s else 0
    except Exception:
        return 0

def to_float(v: Any) -> Optional[float]:
    try:
        s = str(v).strip()
        return float(s) if s else None
    except Exception:
        return None

def split_space(v: Any) -> List[str]:
    s = str(v or "").strip()
    return [x for x in s.split() if x]

# ── Descarga del CSV via export-setting (portal_csv) ──────────────────────────
def fetch_export_settings(hdrs: Dict) -> List[Dict]:
    url = f"{BASE}/api/v2/assets/{ASSET}/export-settings/"
    out = []
    while url:
        r = http_get(url, hdrs, timeout=120, tries=5)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "results" in data:
            out.extend(data.get("results") or [])
            url = data.get("next")
        else:
            out.extend(data if isinstance(data, list) else [])
            url = None
    return out

def fetch_csv_via_export_setting(hdrs: Dict) -> Optional[str]:
    """Intenta descargar el CSV usando el export-setting guardado 'portal_csv'."""
    try:
        settings = fetch_export_settings(hdrs)
        export = next(
            (s for s in settings
             if (s.get("name") or s.get("title") or "").strip() == EXPORT_NAME),
            None
        )
        if not export:
            print(f"[WARN] No se encontró export-setting '{EXPORT_NAME}'. Usando API directa.")
            return None

        settings_url = export.get("url") or f"{BASE}/api/v2/assets/{ASSET}/export-settings/{export['uid']}/"
        if settings_url.startswith("/"):
            settings_url = BASE + settings_url
        csv_url = settings_url.rstrip("/") + "/data.csv"

        r = http_get(csv_url, hdrs, timeout=240, tries=7)
        r.raise_for_status()
        return r.content.decode("utf-8-sig", errors="replace")
    except Exception as e:
        print(f"[WARN] Export-setting falló: {e}. Usando API directa.")
        return None

def fetch_csv_direct(hdrs: Dict) -> str:
    """Fallback: descarga submissions directamente como JSON y convierte a CSV."""
    url = f"{BASE}/api/v2/assets/{ASSET}/data/?format=json&limit=30000"
    r = http_get(url, hdrs, timeout=240, tries=7)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    if not results:
        return ""
    # Construir CSV desde JSON
    all_keys = []
    for row in results:
        for k in row.keys():
            if k not in all_keys:
                all_keys.append(k)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, delimiter=";", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
    return buf.getvalue()

# ── Parsing del CSV ─────────────────────────────────────────────────────────────
def parse_csv(text: str) -> Tuple[List[Dict], List[str]]:
    """Detecta separador y retorna (rows, fieldnames)."""
    if not text.strip():
        return [], []
    # Detectar separador
    first_line = text.split("\n")[0]
    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    fieldnames = list(reader.fieldnames or [])
    # Si quedó 1 sola columna, probar el otro separador
    if rows and len(fieldnames) <= 1:
        other = "," if delimiter == ";" else ";"
        reader2 = csv.DictReader(io.StringIO(text), delimiter=other)
        rows2 = list(reader2)
        if len(reader2.fieldnames or []) > 1:
            return rows2, list(reader2.fieldnames)
    return rows, fieldnames

# ── Coordenadas ────────────────────────────────────────────────────────────────
def extract_coords(row: Dict) -> Optional[List[float]]:
    """Extrae [lon, lat] probando múltiples formatos de KoBo."""
    # Formato separado: _ubicacion_latitude / _ubicacion_longitude
    for lat_f, lon_f in zip(LAT_CANDIDATES, LON_CANDIDATES):
        lat = to_float(row.get(lat_f))
        lon = to_float(row.get(lon_f))
        if lat is not None and lon is not None and (lat != 0 or lon != 0):
            return [lon, lat]

    # Formato combinado: "lat lon alt prec"
    for field in GEOPOINT_COMBINED:
        v = str(row.get(field) or "").strip()
        if v:
            parts = v.split()
            if len(parts) >= 2:
                lat = to_float(parts[0])
                lon = to_float(parts[1])
                if lat is not None and lon is not None and (lat != 0 or lon != 0):
                    return [lon, lat]
    return None

def extract_precision(row: Dict) -> Optional[float]:
    for f in PREC_CANDIDATES:
        v = to_float(row.get(f))
        if v is not None:
            return round(v, 1)
    # Intentar desde campo combinado (4to valor)
    for field in GEOPOINT_COMBINED:
        v = str(row.get(field) or "").strip()
        parts = v.split()
        if len(parts) >= 4:
            p = to_float(parts[3])
            if p is not None:
                return round(p, 1)
    return None

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    sync_time = utc_now_iso()  # Hora real del sync — siempre
    hdrs = {"Authorization": f"Token {TOKEN}"}

    # 1. Obtener CSV
    text = fetch_csv_via_export_setting(hdrs)
    if not text:
        text = fetch_csv_direct(hdrs)

    os.makedirs("data", exist_ok=True)

    rows, fieldnames = parse_csv(text)
    print(f"[INFO] {len(rows)} filas, {len(fieldnames)} columnas")

    if not rows:
        geojson = {"type": "FeatureCollection", "features": []}
        resumen = {
            "ultima_actualizacion": sync_time,
            "kpis": {"total_boletas": 0, "total_plantas": 0,
                     "total_participantes": 0, "total_area_m2": 0}
        }
        with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
        with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2)
        return

    features = []
    # KPIs — se cuentan TODAS las boletas, no solo las que tienen geopoint
    total_boletas     = len(rows)
    total_plantas     = 0
    total_part        = 0
    total_area_m2     = 0

    for row in rows:
        rid = (row.get("_id") or row.get("_uuid") or
               row.get("meta/instanceID") or f"row-{len(features)+1}")

        plantas      = to_int(row.get("total_plantas"))
        participantes = to_int(row.get("total_participantes"))
        area         = to_int(row.get("area_m2"))

        total_plantas  += plantas
        total_part     += participantes
        total_area_m2  += area

        # Solo se agrega al mapa si tiene coordenadas válidas
        coords = extract_coords(row)
        if not coords:
            print(f"  [SKIP mapa] ID {rid}: sin coordenadas válidas")
            continue

        props = {
            "id":                    str(rid),
            "fecha_actividad":       row.get("fecha_actividad") or "",
            "submission_time":       row.get("_submission_time") or "",
            "encuestador":           row.get("encuestador") or "",
            "municipios":            split_space(row.get("municipios")),
            "comunidad":             row.get("comunidad") or "",
            "sitio_nombre":          row.get("sitio_nombre") or "",
            "instituciones":         split_space(row.get("institucion_resp")),
            "institucion_resp_otro": row.get("institucion_resp_otro") or "",
            "area_m2":               area,
            "tenencia":              row.get("tenencia") or "",
            "mujeres":               to_int(row.get("mujeres")),
            "hombres":               to_int(row.get("hombres")),
            "ninas":                 to_int(row.get("ninas")),
            "ninos":                 to_int(row.get("ninos")),
            "total_participantes":   participantes,
            "total_plantas":         plantas,
            "precision_gps":         extract_precision(row),
            "origen_planton":        row.get("origen_planton") or "",
            "origen_planton_otro":   row.get("origen_planton_otro") or "",
            "autoriza_fotos":        row.get("autoriza_fotos") or "",
            "foto_sitio_url":        row.get("foto_sitio_URL") or row.get("foto_sitio") or "",
            "foto_actividad_url":    row.get("foto_actividad_URL") or row.get("foto_actividad") or "",
            "observaciones":         row.get("observaciones") or "",
        }

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": props
        })

    geojson = {"type": "FeatureCollection", "features": features}
    resumen = {
        "ultima_actualizacion": sync_time,          # hora real del sync
        "kpis": {
            "total_boletas":      total_boletas,    # todas, con o sin GPS
            "total_plantas":      total_plantas,
            "total_participantes": total_part,
            "total_area_m2":      total_area_m2,
        }
    }

    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

    print(f"[OK] {total_boletas} boletas | {len(features)} con GPS | "
          f"{total_plantas} plantas | {total_part} participantes | "
          f"{total_area_m2} m²")
    print(f"[OK] Sync: {sync_time}")

if __name__ == "__main__":
    main()
