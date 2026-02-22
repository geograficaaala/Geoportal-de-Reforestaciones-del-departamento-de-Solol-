import os
import csv
import io
import json
import time
import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

BASE = os.getenv("KOBO_BASE_URL", "https://kf.kobotoolbox.org").rstrip("/")
TOKEN = os.environ["KOBO_TOKEN"]
ASSET = os.environ["KOBO_ASSET_UID"]
EXPORT_NAME = os.getenv("KOBO_EXPORT_NAME", "portal_csv")

OUT_GEOJSON = "data/puntos.geojson"
OUT_RESUMEN = "data/resumen.json"

GEOPOINT_ROOT_CANDIDATES = ["ubicacion", "_geolocation", "geopoint", "location"]
DATE_FIELD_CANDIDATES = ["fecha_actividad", "_submission_time", "endtime", "starttime", "today", "start", "end"]

def utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def http_get_with_retries(url: str, headers: Dict[str, str], timeout: int = 180, tries: int = 7) -> requests.Response:
    last_err = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code in (502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} temporary", response=r)
            return r
        except Exception as e:
            last_err = e
            time.sleep(min(30, 3 * (2 ** i)))
    raise RuntimeError(f"Fallo al descargar tras reintentos. URL: {url}. Error: {last_err}")

def fetch_all_export_settings(headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"{BASE}/api/v2/assets/{ASSET}/export-settings/"
    out: List[Dict[str, Any]] = []
    while url:
        r = http_get_with_retries(url, headers=headers, timeout=120, tries=5)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "results" in data:
            out.extend(data.get("results") or [])
            url = data.get("next")
        elif isinstance(data, list):
            out.extend(data)
            url = None
        else:
            url = None
    return out

def build_data_csv_url(export_setting: Dict[str, Any]) -> str:
    # Usar endpoint síncrono estable: .../export-settings/<ID>/data.csv
    settings_url = export_setting.get("url")
    if not settings_url:
        uid = export_setting.get("uid")
        if uid:
            settings_url = f"/api/v2/assets/{ASSET}/export-settings/{uid}/"
        else:
            raise RuntimeError("El export-setting no trae 'url' ni 'uid'.")
    if settings_url.startswith("/"):
        settings_url = BASE + settings_url
    return settings_url.rstrip("/") + "/data.csv"

def split_multi_text(v: Any) -> List[str]:
    if v is None:
        return []
    s = str(v).strip()
    return s.split() if s else []

def truthy(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "si", "sí")

def multiselect_from_split_columns(row: Dict[str, Any], base: str) -> List[str]:
    # Base/choice=1 o base_choice=1
    out = []
    for k, v in row.items():
        if k.startswith(base + "/") and truthy(v):
            out.append(k.split("/", 1)[1])
        elif k.startswith(base + "_") and truthy(v):
            out.append(k.split(base + "_", 1)[1])
    return out

def get_multiselect(row: Dict[str, Any], base: str) -> List[str]:
    if base in row and str(row.get(base) or "").strip():
        return split_multi_text(row.get(base))
    return multiselect_from_split_columns(row, base)

def to_int(v: Any) -> int:
    try:
        s = str(v).strip()
        if not s:
            return 0
        return int(round(float(s)))
    except Exception:
        return 0

def iso_parse(v: Any) -> Optional[datetime.datetime]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None

def sniff_dialect(text: str) -> csv.Dialect:
    sample = text[:4096]
    try:
        d = csv.Sniffer().sniff(sample, delimiters=";,\t")
        return d
    except Exception:
        # fallback común en ES: ;
        d = csv.excel
        d.delimiter = ";"
        return d

def find_geopoint_mode(headers: List[str]) -> Tuple[str, str, Optional[str]]:
    hset = set(headers)
    for root in GEOPOINT_ROOT_CANDIDATES:
        # combinado: ubicacion = "lat lon ..."
        if root in hset:
            return ("combined", root, None)

        # separados (varios patrones)
        cand_pairs = [
            (f"{root}_latitude", f"{root}_longitude"),
            (f"{root}/latitude", f"{root}/longitude"),
            (f"_{root}_latitude", f"_{root}_longitude"),  # KoBo a veces crea _ubicacion_latitude
        ]
        for latf, lonf in cand_pairs:
            if latf in hset and lonf in hset:
                return ("split", latf, lonf)

    raise RuntimeError(f"No encontré geopoint. Probé: {GEOPOINT_ROOT_CANDIDATES} y variantes *_latitude/_longitude.")

def parse_coords(row: Dict[str, Any], mode: Tuple[str, str, Optional[str]]) -> Optional[List[float]]:
    kind, a, b = mode
    if kind == "combined":
        v = row.get(a)
        if v is None:
            return None
        parts = str(v).strip().split()
        if len(parts) < 2:
            return None
        try:
            lat = float(parts[0])
            lon = float(parts[1])
            return [lon, lat]
        except Exception:
            return None
    else:
        try:
            lat = float(str(row.get(a) or "").strip())
            lon = float(str(row.get(b) or "").strip()) if b else None
            if lon is None:
                return None
            return [lon, lat]
        except Exception:
            return None

def main():
    headers = {"Authorization": f"Token {TOKEN}"}

    settings = fetch_all_export_settings(headers)
    export = None
    for it in settings:
        name = (it.get("name") or it.get("title") or "").strip()
        if name == EXPORT_NAME:
            export = it
            break
    if export is None:
        raise RuntimeError(f"No encontré export-setting con name='{EXPORT_NAME}'.")

    csv_url = build_data_csv_url(export)
    r = http_get_with_retries(csv_url, headers=headers, timeout=240, tries=7)
    r.raise_for_status()

    text = r.content.decode("utf-8-sig", errors="replace")

    dialect = sniff_dialect(text)
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows = list(reader)

    # Si el dialecto falló y quedó 1 sola columna, intenta con ';'
    if rows and reader.fieldnames and len(reader.fieldnames) == 1:
        dialect = csv.excel
        dialect.delimiter = ";"
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        rows = list(reader)

    os.makedirs("data", exist_ok=True)

    if not rows:
        geojson = {"type": "FeatureCollection", "features": []}
        resumen = {
            "ultima_actualizacion": utc_now_iso(),
            "kpis": {"total_boletas": 0, "total_plantas": 0, "total_participantes": 0},
        }
        with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
        with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2)
        return

    headers_csv = list(rows[0].keys())
    geopoint_mode = find_geopoint_mode(headers_csv)

    date_field = None
    for k in DATE_FIELD_CANDIDATES:
        if k in rows[0]:
            date_field = k
            break

    features = []
    total_boletas = 0
    total_plantas = 0
    total_part = 0
    last_ts: Optional[datetime.datetime] = None

    for row in rows:
        coords = parse_coords(row, geopoint_mode)
        if not coords:
            continue

        rid = row.get("_id") or row.get("_uuid") or row.get("meta/instanceID") or row.get("id") or f"row-{len(features)+1}"

        municipios = get_multiselect(row, "municipios")
        instituciones = get_multiselect(row, "institucion_resp")

        props = {
            "id": rid,
            "fecha_actividad": row.get("fecha_actividad") or (row.get(date_field) if date_field else "") or "",
            "municipios": municipios,
            "comunidad": row.get("comunidad") or "",
            "sitio_nombre": row.get("sitio_nombre") or "",
            "instituciones": instituciones,
            "institucion_resp_otro": row.get("institucion_resp_otro") or "",
            "area_m2": to_int(row.get("area_m2")),
            "tenencia": row.get("tenencia") or "",
            "total_plantas": to_int(row.get("total_plantas")),
            "total_participantes": to_int(row.get("total_participantes")),
            "autoriza_fotos": row.get("autoriza_fotos") or "",
            # KoBo suele crear columnas *_URL
            "foto_sitio_url": row.get("foto_sitio_URL") or row.get("foto_sitio") or "",
            "foto_actividad_url": row.get("foto_actividad_URL") or row.get("foto_actividad") or "",
            "observaciones": row.get("observaciones") or "",
        }

        total_boletas += 1
        total_plantas += props["total_plantas"]
        total_part += props["total_participantes"]

        if date_field:
            ts = iso_parse(row.get(date_field))
            if ts and (last_ts is None or ts > last_ts):
                last_ts = ts

        features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": coords}, "properties": props})

    geojson = {"type": "FeatureCollection", "features": features}
    ultima = (last_ts.replace(microsecond=0).isoformat() if last_ts else utc_now_iso())

    resumen = {
        "ultima_actualizacion": ultima,
        "kpis": {"total_boletas": int(total_boletas), "total_plantas": int(total_plantas), "total_participantes": int(total_part)},
    }

    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
