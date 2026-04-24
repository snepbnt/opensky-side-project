import os
import threading
import time
import logging
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv
from pyiceberg.catalog.glue import GlueCatalog

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# app.py 와 같은 폴더에서 index.html 찾기
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=BASE_DIR)
CORS(app)

# ── 설정 ────────────────────────────────────────────────────────────────────
AWS_REGION      = os.getenv("AWS_REGION", "ap-northeast-2")
GLUE_DATABASE   = os.getenv("GLUE_DATABASE", "flight_db")
GLUE_TABLE      = os.getenv("GLUE_TABLE",    "flight_states")
S3_WAREHOUSE    = os.getenv("S3_WAREHOUSE")          # s3://your-bucket/iceberg/
PULL_INTERVAL   = int(os.getenv("PULL_INTERVAL", "30"))  # 초 단위

# ── 인메모리 캐시 ──────────────────────────────────────────────────────────
_cache = {
    "flights":    [],
    "updated_at": None,
    "count":      0,
    "error":      None,
}
_lock = threading.Lock()


# ── Iceberg → dict 변환 ───────────────────────────────────────────────────
REQUIRED_COLS = [
    "icao24", "callsign", "origin_country",
    "longitude", "latitude", "baro_altitude",
    "on_ground", "velocity", "true_track",
    "vertical_rate", "geo_altitude", "time_position",
]

def _read_latest_iceberg() -> list[dict]:
    """S3 Iceberg 테이블에서 최신 스냅샷 읽기."""
    catalog = GlueCatalog(
        name="glue",
        **{
            "region_name": AWS_REGION,
            "warehouse":   S3_WAREHOUSE,
        },
    )
    table = catalog.load_table(f"{GLUE_DATABASE}.{GLUE_TABLE}")
    scan  = table.scan(selected_fields=tuple(REQUIRED_COLS))
    arrow = scan.to_arrow()

    if len(arrow) == 0:
        return []

    df = arrow.to_pandas()
    df = df.dropna(subset=["latitude", "longitude"])

    # icao24 기준 가장 최신 레코드만 남기기
    if "time_position" in df.columns:
        df = df.sort_values("time_position", ascending=False)
        df = df.drop_duplicates(subset=["icao24"], keep="first")

    records = []
    for _, row in df.iterrows():
        records.append({
            "icao24":         str(row.get("icao24", "") or ""),
            "callsign":       str(row.get("callsign", "") or "").strip(),
            "origin_country": str(row.get("origin_country", "") or ""),
            "lat":            float(row["latitude"]),
            "lon":            float(row["longitude"]),
            "altitude":       _safe_float(row.get("baro_altitude")),
            "geo_altitude":   _safe_float(row.get("geo_altitude")),
            "velocity":       _safe_float(row.get("velocity")),
            "heading":        _safe_float(row.get("true_track")),
            "vertical_rate":  _safe_float(row.get("vertical_rate")),
            "on_ground":      bool(row.get("on_ground", False)),
            "time_position":  _safe_int(row.get("time_position")),
        })
    return records


def _safe_float(v):
    try:
        return round(float(v), 2) if v is not None else None
    except Exception:
        return None

def _safe_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


# ── 백그라운드 폴러 ───────────────────────────────────────────────────────
def _pull_loop():
    while True:
        try:
            log.info("S3 Iceberg pull 시작...")
            flights = _read_latest_iceberg()
            with _lock:
                _cache["flights"]    = flights
                _cache["updated_at"] = datetime.now(timezone.utc).isoformat()
                _cache["count"]      = len(flights)
                _cache["error"]      = None
            log.info(f"Pull 완료 — {len(flights)}개 항공기")
        except Exception as e:
            log.error(f"Pull 실패: {e}")
            with _lock:
                _cache["error"] = str(e)
        time.sleep(PULL_INTERVAL)


# ── API 라우트 ────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html", pull_interval=PULL_INTERVAL)

@app.route("/api/flights")
def api_flights():
    with _lock:
        return jsonify({
            "flights":    _cache["flights"],
            "updated_at": _cache["updated_at"],
            "count":      _cache["count"],
            "error":      _cache["error"],
        })

@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify({
            "updated_at":    _cache["updated_at"],
            "count":         _cache["count"],
            "pull_interval": PULL_INTERVAL,
            "error":         _cache["error"],
        })


# ── 진입점 ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t = threading.Thread(target=_pull_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5001, debug=False)