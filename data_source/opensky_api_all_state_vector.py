import json
import os
import time
import logging
import boto3
import urllib3

# 로그 설정 (로컬 콘솔에서 흐름을 보기 위해 출력 설정)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# ── 환경변수 설정 (로컬 환경에 맞춰 직접 입력하거나 .env 파일을 사용하세요) ──
# 팁: 로컬 테스트 시에는 os.environ.get("키", "기본값") 형태가 편합니다.
CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID", "snepbnt404-api-client")
CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "eBF2ZdjPrKkxEcXNLSIAsdHX2luvDBCB")
STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "opensky-data-stream") # 여기에 스트림 이름 입력
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2") # 예: 서울 리전



# 한국 전체 대신 수도권만
LAMIN = "36.0"
LOMIN = "126.0"
LAMAX = "38.0"
LOMAX = "128.5"

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
API_URL = "https://opensky-network.org/api/states/all"

http = urllib3.PoolManager()

# 로컬 실행 시 리전을 명시적으로 지정하는 것이 오류를 방지하는 가장 좋은 방법입니다.
try:
    kinesis = boto3.client("kinesis", region_name=AWS_REGION)
    # 로컬 테스트용: 현재 인증된 자격 증명으로 스트림에 접근 가능한지 확인
    kinesis.describe_stream(StreamName=STREAM_NAME)
    logger.info(f"Kinesis 스트림 '{STREAM_NAME}'에 연결되었습니다. (Region: {AWS_REGION})")
except Exception as e:
    logger.error(f"Kinesis 연결 실패: {e}")
    # 권한 문제나 리전 문제가 있다면 여기서 에러가 발생합니다.

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
    states = states[:50]
    ts = data.get("time", int(time.time()))
    
    if not states:
        logger.info("보낼 데이터가 없습니다.")
        return 0

    batch = []
    sent_count = 0
    
    # 샘플 출력
    sample = dict(zip(STATE_KEYS, states[0]))
    sample["request_time"] = ts
    print(json.dumps(sample, indent=2, ensure_ascii=False))

    for sv in states:
        record = dict(zip(STATE_KEYS, sv))
        record["request_time"] = ts
        
        batch.append({
            "Data": json.dumps(record, separators=(",", ":")).encode('utf-8'),
            "PartitionKey": str(record["icao24"]), # 파티션 키는 반드시 문자열이어야 합니다.
        })
        
        # PutRecords는 한 번에 최대 500개까지 가능합니다.
        if len(batch) == 3:
            response = kinesis.put_records(StreamName=STREAM_NAME, Records=batch)
            sent_count += len(batch) - response.get('FailedRecordCount', 0)
            logger.info(f"3개 레코드 전송 완료... (누적: {sent_count})")
            batch = []
            time.sleep(0.5) # 로컬 실행 시 쓰로틀링 방지를 위해 약간의 대기

    if batch:
        response = kinesis.put_records(StreamName=STREAM_NAME, Records=batch)
        sent_count += len(batch) - response.get('FailedRecordCount', 0)

    logger.info(f"최종 전송 완료: 총 {sent_count}개의 레코드를 Kinesis로 보냈습니다.")
    return sent_count

if __name__ == '__main__':
    try:
        logger.info("OpenSky API 데이터 가져오는 중...")
        data = _fetch_states()
        
        logger.info("Kinesis로 데이터 전송 시작...")
        count = _send_to_kinesis(data)
        
    except Exception as e:
        logger.error(f"오류 발생: {e}")