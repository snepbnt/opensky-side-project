import json
import os
import time
import logging

import boto3
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── 환경변수 ──
CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID", "snepbnt404-api-client")
CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "eBF2ZdjPrKkxEcXNLSIAsdHX2luvDBCB")
STREAM_NAME = "opensky-data-stream"

# 바운딩 박스 (옵션)
LAMIN = os.environ.get("LAMIN")
LOMIN = os.environ.get("LOMIN")
LAMAX = os.environ.get("LAMAX")
LOMAX = os.environ.get("LOMAX")

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"

http = urllib3.PoolManager()
kinesis = boto3.client("kinesis")

# ── Token 캐시 ──
_token = None
_token_exp = 0


def _get_token() -> str:
    global _token, _token_exp
    if _token and time.time() < _token_exp:
        return _token
    print("통신시작")
    r = http.request(
        "POST", TOKEN_URL,
        fields={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        encode_multipart=False,
    )
    if r.status != 200:
        raise RuntimeError(f"Token error {r.status}: {r.data.decode()[:200]}")

    body = json.loads(r.data)
    _token = body["access_token"]
    _token_exp = time.time() + body.get("expires_in", 1800) - 60
    print("통신종료")

    return _token


def _fetch_states() -> dict:
    params = []
    if all([LAMIN, LOMIN, LAMAX, LOMAX]):
        params += [
            ("lamin", LAMIN), ("lomin", LOMIN),
            ("lamax", LAMAX), ("lomax", LOMAX),
        ]

    qs = "&".join(f"{k}={v}" for k, v in params)
    url = API_URL + (f"?{qs}" if qs else "")

    headers = {}
    if CLIENT_ID and CLIENT_SECRET:
        headers["Authorization"] = f"Bearer {_get_token()}"

    r = http.request("GET", url, headers=headers, timeout=30.0)
    if r.status != 200:
        raise RuntimeError(f"API error {r.status}: {r.data.decode()[:300]}")
    return json.loads(r.data)


STATE_KEYS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


def _send_to_kinesis(data: dict) -> int:
    states = data.get("states") or []
    ts = data.get("time", int(time.time()))

    batch = []
    for sv in states:
        record = dict(zip(STATE_KEYS, sv))
        record["request_time"] = ts
        batch.append({
            "Data": json.dumps(record, separators=(",", ":")).encode(),
            "PartitionKey": record["icao24"],
        })
        if len(batch) == 500:  # PutRecords 최대 500건
            kinesis.put_records(StreamName=STREAM_NAME, Records=batch)
            batch = []
            time.sleep(3)

    if batch:
        kinesis.put_records(StreamName=STREAM_NAME, Records=batch)

    logger.info("Sent %d records to Kinesis (t=%d)", len(states), ts)
    return len(states)


if __name__=='__main__':
    data = _fetch_states()
    print(data)
    