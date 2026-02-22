import os
import csv
import io
import json
import time
import datetime
from typing import Any, Dict, List, Optional

import requests

# =========================
# CONFIG (desde Secrets)
# =========================
BASE = os.getenv("KOBO_BASE_URL", "https://kf.kobotoolbox.org").rstrip("/")
TOKEN = os.environ["KOBO_TOKEN"]
ASSET = os.environ["KOBO_ASSET_UID"]
EXPORT_NAME = os.getenv("KOBO_EXPORT_NAME", "portal_csv")

OUT_GEOJSON = "data/puntos.geojson"
OUT_RESUMEN = "data/resumen.json"

# Campos típicos (ajustados a tu boleta)
GEOPOINT_FIELD_CANDIDATES = ["ubicacion", "_geolocation", "geopoint", "location"]
DATE_FIELD_CANDIDATES = [
    "fecha_actividad",
    "_submission_time",
    "endtime",
    "starttime",
    "today",
    "start",
    "end",
]

TOTAL_PLANTAS_CANDIDATES = ["total_plantas"]
TOTAL_PART_CANDIDATES = ["total_participantes"]

# =========================
# HELPERS
# =========================
def utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def first_existing_key(headers: List[str], candidates: List[str]) -> Optional[str]:
    hset = set(headers)
    for k in candidates:
        if k in hset:
            return k
    return None

def parse_geopoint(v: Any) -> Optional[List[float]]:
    """
    KoBo CSV geopoint suele venir como: "lat lon alt acc" o "lat lon"
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    parts = s.split()
    if len(parts) < 2:
        return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
        # GeoJSON usa [lon, lat]
        return [lon, lat]
    except Exception:
        return None

def split_multi(v: Any) -> List[str]:
    # select_multiple en CSV suele venir separado por espacios
    if v is None:
        return []
    s = str(v).strip()
    return s.split() if s else []

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

def http_get_with_retries(url: str, headers: Dict[str, str], timeout: int = 180, tries: int = 6) -> requests.Response:
    """
    Reintentos para manejar 502/503/504 o latencia de KoBo.
    """
    last_err = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            # Si KoBo anda inestable, puede devolver 502/503/504
            if r.status_code in (502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} temporary", response=r)
            return r
        except Exception as e:
            last_err = e
            # backoff: 3, 6, 12, 20, 30...
            sleep_s = min(30, 3 * (2 ** i))
            time.sleep(sleep_s)
    raise RuntimeError(f"Fallo al descargar tras reintentos. URL: {url}. Error: {last_err}")

def fetch_all_export_settings(headers: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Trae todos los export-settings (maneja paginación si existe).
    """
    url = f"{BASE}/api/v2/assets/{ASSET}/export-settings/"
    out: List[Dict[str, Any]] = []

    while url:
        r = http_get_with_retries(url, headers=headers, timeout=120, tries=5)
        r.raise_for_status()
        data = r.json()

        # Puede venir como {results:[...], next:...} o como [...]
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
    """
    Fuerza el endpoint estable:
      .../export-settings/<ID>/data.csv
    En vez de links /private-media/... que a veces dan 502.
    """
    settings_url = export_setting.get("url")

    if not settings_url:
        # Fallback: a veces trae uid
        uid = export_setting.get("uid")
        if uid:
            settings_url = f"/api/v2/assets/{ASSET}/export-settings/{uid}/"
        else:
            raise RuntimeError("El export-setting no trae 'url' ni 'uid'. Revisa KoBo export-settings.")

    if settings_url.startswith("/"):
        settings_url = BASE + settings_url

    return settings_url.rstrip("/") + "/data.csv"

# =========================
# MAIN
# =========================
def main():
    headers = {"Authorization": f"Token {TOKEN}"}

    # 1) Buscar export-setting por nombre
    settings = fetch_all_export_settings(headers)
    if not settings:
        raise RuntimeError("No encontré export-settings en KoBo. Revisa que hayas guardado 'Guardar selección como...'.")

    export = None
    available = []
    for it in settings:
        name = (it.get("name") or it.get("title") or "").strip()
        if name:
            available.append(name)
        if name == EXPORT_NAME:
            export = it

    if export is None:
        raise RuntimeError(
            f"No encontré un export-setting con name='{EXPORT_NAME}'. "
            f"Nombres disponibles: {available[:20]}{'...' if len(available) > 20 else ''}"
        )

    # 2) Descargar CSV desde endpoint estable
    csv_url = build_data_csv_url(export)
    r = http_get_with_retries(csv_url, headers=headers, timeout=240, tries=7)
    r.raise_for_status()

    text = r.content.decode("utf-8-sig", errors="replace")  # maneja BOM
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    # 3) Si no hay datos, escribe archivos vacíos pero válidos
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

    # 4) Detectar nombres de columnas reales
    headers_csv = list(rows[0].keys())

    geopoint_field = first_existing_key(headers_csv, GEOPOINT_FIELD_CANDIDATES)
    if not geopoint_field:
        raise RuntimeError(
            f"No encontré campo geopoint. Busqué: {GEOPOINT_FIELD_CANDIDATES}. "
            f"Columnas disponibles (primeras 40): {headers_csv[:40]}"
        )

    date_field = first_existing_key(headers_csv, DATE_FIELD_CANDIDATES)
    plantas_field = first_existing_key(headers_csv, TOTAL_PLANTAS_CANDIDATES)
    part_field = first_existing_key(headers_csv, TOTAL_PART_CANDIDATES)

    # 5) Construir GeoJSON + KPIs
    features = []
    total_boletas = 0
    total_plantas = 0
    total_part = 0
    last_ts: Optional[datetime.datetime] = None

    for row in rows:
        coords = parse_geopoint(row.get(geopoint_field))
        if not coords:
            continue

        rid = (
            row.get("_id")
            or row.get("_uuid")
            or row.get("meta/instanceID")
            or row.get("id")
            or f"row-{len(features)+1}"
        )

        # Campos principales (con fallback)
        props = {
            "id": rid,
            "fecha_actividad": row.get("fecha_actividad") or (row.get(date_field) if date_field else "") or "",
            "municipios": split_multi(row.get("municipios")),
            "comunidad": row.get("comunidad") or "",
            "sitio_nombre": row.get("sitio_nombre") or "",
            "instituciones": split_multi(row.get("institucion_resp")),
            "institucion_resp_otro": row.get("institucion_resp_otro") or "",
            "area_m2": to_int(row.get("area_m2")),
            "tenencia": row.get("tenencia") or "",
            "total_plantas": to_int(row.get(plantas_field)) if plantas_field else to_int(row.get("total_plantas")),
            "total_participantes": to_int(row.get(part_field)) if part_field else to_int(row.get("total_participantes")),
            "autoriza_fotos": row.get("autoriza_fotos") or "",
            # si en tu CSV salen URLs (porque marcaste "Incluir URL de archivos multimedia"), aquí quedarán:
            "foto_sitio_url": row.get("foto_sitio") or "",
            "foto_actividad_url": row.get("foto_actividad") or "",
            "observaciones": row.get("observaciones") or "",
        }

        total_boletas += 1
        total_plantas += props["total_plantas"]
        total_part += props["total_participantes"]

        # última actualización (si hay campo ISO)
        if date_field:
            ts = iso_parse(row.get(date_field))
            if ts and (last_ts is None or ts > last_ts):
                last_ts = ts

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": coords},
                "properties": props,
            }
        )

    geojson = {"type": "FeatureCollection", "features": features}
    ultima = (last_ts.replace(microsecond=0).isoformat() if last_ts else utc_now_iso())

    resumen = {
        "ultima_actualizacion": ultima,
        "kpis": {
            "total_boletas": int(total_boletas),
            "total_plantas": int(total_plantas),
            "total_participantes": int(total_part),
        },
    }

    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
