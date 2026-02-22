import os, csv, io, json, datetime
import requests

BASE = os.getenv("KOBO_BASE_URL", "https://kf.kobotoolbox.org").rstrip("/")
TOKEN = os.environ["KOBO_TOKEN"]
ASSET = os.environ["KOBO_ASSET_UID"]
EXPORT_NAME = os.getenv("KOBO_EXPORT_NAME", "portal_csv")

OUT_GEOJSON = "data/puntos.geojson"
OUT_RESUMEN = "data/resumen.json"

GEOPOINT_FIELD_CANDIDATES = ["ubicacion", "_geolocation", "geopoint", "location"]
DATE_FIELD_CANDIDATES = ["fecha_actividad", "_submission_time", "start", "today"]
TOTAL_PLANTAS_CANDIDATES = ["total_plantas"]
TOTAL_PART_CANDIDATES = ["total_participantes"]

def first_existing(row, keys):
    for k in keys:
        if k in row and str(row.get(k) or "").strip():
            return k
    return None

def parse_geopoint(v):
    # KoBo CSV: "lat lon alt acc" o "lat lon"
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    parts = s.split()
    if len(parts) < 2:
        return None
    try:
        lat = float(parts[0]); lon = float(parts[1])
        return [lon, lat]
    except:
        return None

def split_multi(v):
    if v is None:
        return []
    s = str(v).strip()
    return s.split() if s else []

def to_int(v):
    try:
        s = str(v).strip()
        if not s:
            return 0
        return int(round(float(s)))
    except:
        return 0

def iso_parse(v):
    if not v:
        return None
    s = str(v).strip().replace("Z", "+00:00")
    try:
        return datetime.datetime.fromisoformat(s)
    except:
        return None

def main():
    headers = {"Authorization": f"Token {TOKEN}"}

    # 1) Buscar export-settings y ubicar el export guardado "portal_csv"
    url_es = f"{BASE}/api/v2/assets/{ASSET}/export-settings/"
    r = requests.get(url_es, headers=headers, timeout=120)
    r.raise_for_status()
    data = r.json()

    items = data.get("results", data if isinstance(data, list) else [])
    export = None
    for it in items:
        name = (it.get("name") or it.get("title") or "").strip()
        if name == EXPORT_NAME:
            export = it
            break
    if export is None:
        raise RuntimeError(f"No encontré un export-settings con name='{EXPORT_NAME}'. Revisa el nombre en KoBo.")

    csv_url = export.get("data_url_csv") or export.get("data_url") or export.get("url")
    if not csv_url:
        raise RuntimeError("El export-settings no trae data_url_csv/data_url. Vuelve a generar el export en KoBo.")
    if csv_url.startswith("/"):
        csv_url = BASE + csv_url

    # 2) Descargar CSV
    r = requests.get(csv_url, headers=headers, timeout=180)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig", errors="replace")  # utf-8 con BOM a veces

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        geojson = {"type": "FeatureCollection", "features": []}
        resumen = {
            "ultima_actualizacion": datetime.datetime.utcnow().isoformat() + "Z",
            "kpis": {"total_boletas": 0, "total_plantas": 0, "total_participantes": 0},
        }
        os.makedirs("data", exist_ok=True)
        with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
        with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2)
        return

    sample = rows[0]
    geopoint_field = first_existing(sample, GEOPOINT_FIELD_CANDIDATES) or GEOPOINT_FIELD_CANDIDATES[0]
    date_field = first_existing(sample, DATE_FIELD_CANDIDATES)
    plantas_field = first_existing(sample, TOTAL_PLANTAS_CANDIDATES)
    part_field = first_existing(sample, TOTAL_PART_CANDIDATES)

    features = []
    total_boletas = 0
    total_plantas = 0
    total_part = 0
    last_ts = None

    for row in rows:
        coords = parse_geopoint(row.get(geopoint_field))
        if not coords:
            continue

        rid = row.get("_id") or row.get("_uuid") or row.get("meta/instanceID") or row.get("id") or f"row-{len(features)+1}"

        props = {
            "id": rid,
            "fecha_actividad": row.get("fecha_actividad") or row.get(date_field) or "",
            "municipios": split_multi(row.get("municipios")),
            "comunidad": row.get("comunidad") or "",
            "sitio_nombre": row.get("sitio_nombre") or row.get("sitio_nombre") or "",
            "instituciones": split_multi(row.get("institucion_resp")),
            "institucion_resp_otro": row.get("institucion_resp_otro") or "",
            "area_m2": to_int(row.get("area_m2")),
            "tenencia": row.get("tenencia") or "",
            "total_plantas": to_int(row.get(plantas_field)) if plantas_field else to_int(row.get("total_plantas")),
            "total_participantes": to_int(row.get(part_field)) if part_field else to_int(row.get("total_participantes")),
            "autoriza_fotos": row.get("autoriza_fotos") or "",
            "foto_sitio_url": row.get("foto_sitio") or "",
            "foto_actividad_url": row.get("foto_actividad") or "",
            "observaciones": row.get("observaciones") or "",
        }

        # KPIs
        total_boletas += 1
        total_plantas += props["total_plantas"]
        total_part += props["total_participantes"]

        # última actualización (si existe un campo fecha/submit)
        ts = iso_parse(row.get(date_field)) if date_field else None
        if ts and (last_ts is None or ts > last_ts):
            last_ts = ts

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": props
        })

    geojson = {"type": "FeatureCollection", "features": features}
    ultima = (last_ts.isoformat() if last_ts else datetime.datetime.utcnow().isoformat() + "Z")

    resumen = {
        "ultima_actualizacion": ultima,
        "kpis": {
            "total_boletas": total_boletas,
            "total_plantas": int(total_plantas),
            "total_participantes": int(total_part),
        },
    }

    os.makedirs("data", exist_ok=True)
    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    with open(OUT_RESUMEN, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
