"""
OpenSky Flight Data Pipeline
Kinesis Data Streams → Amazon Managed Service for Apache Flink → S3 Iceberg

[패키징 구조]
opensky_job.zip
├── flink_opensky_job.py
└── lib/
    ├── iceberg-flink-runtime-1.19-1.6.1.jar
    └── iceberg-aws-bundle-1.6.1.jar

[Environment Properties]
Group: kinesis.analytics.flink.run.options
  - python: flink_opensky_job.py

Group: FlinkApplicationProperties
  - kinesis.stream: opensky-stream
  - aws.region: ap-northeast-2
  - s3.bucket: opensky-jin-data
  - glue.database: opensky_db
  - glue.table: flight_states
  - checkpoint.interval.ms: 60000
"""

import json
import os
import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JAR_FILES = [
    "iceberg-flink-runtime-1.19-1.6.1.jar",
    "iceberg-aws-bundle-1.6.1.jar",
]


# ────────────────────────────────────────────────
# Application Properties 로드
# ────────────────────────────────────────────────
def get_app_properties() -> dict:
    props_file = "/etc/flink/application_properties.json"
    if os.path.exists(props_file):
        with open(props_file, "r") as f:
            groups = json.load(f)
        props = {}
        for group in groups:
            props.update(group.get("PropertyMap", {}))
        logger.info("Managed Flink application properties loaded.")
        return props

    logger.info("Local mode: using environment variables.")
    return {
        "kinesis.stream":         os.environ.get("KINESIS_STREAM", "opensky-stream"),
        "aws.region":             os.environ.get("AWS_REGION", "ap-northeast-2"),
        "s3.bucket":              os.environ.get("S3_BUCKET", "opensky-jin-data"),
        "glue.database":          os.environ.get("GLUE_DATABASE", "opensky_database"),
        "glue.table":             os.environ.get("GLUE_TABLE", "flight_states"),
        "checkpoint.interval.ms": os.environ.get("CHECKPOINT_INTERVAL_MS", "60000"),
    }


# ────────────────────────────────────────────────
# JAR URI 생성 및 존재 검증
# ────────────────────────────────────────────────
def get_jar_uris() -> list:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    lib_dir  = os.path.join(base_dir, "lib")
    uris = []
    for jar in JAR_FILES:
        path = os.path.join(lib_dir, jar)
        if not os.path.exists(path):
            raise FileNotFoundError(f"JAR not found: {path}")
        uris.append(f"file://{path}")
    logger.info("JAR dir: %s", lib_dir)
    return uris


# ────────────────────────────────────────────────
# Flink 환경 초기화
# ────────────────────────────────────────────────
def setup_environment(props: dict) -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(*get_jar_uris())

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    config = t_env.get_config().get_configuration()

    bucket = props["s3.bucket"]
    config.set_string(
        "execution.checkpointing.interval",
        props.get("checkpoint.interval.ms", "60000")
    )
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set_string("execution.checkpointing.timeout", "600000")
    config.set_string(
        "state.checkpoints.dir",
        f"s3://{bucket}/flink-checkpoints/opensky"
    )
    config.set_string("table.dynamic-table-options.enabled", "true")

    logger.info("Flink environment initialized.")
    return t_env


# ────────────────────────────────────────────────
# Glue Catalog 등록
# ────────────────────────────────────────────────
def register_glue_catalog(t_env: StreamTableEnvironment, props: dict):
    bucket = props["s3.bucket"]
    region = props["aws.region"]

    t_env.execute_sql(f"""
        CREATE CATALOG glue_catalog WITH (
            'type'         = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
            'warehouse'    = 's3://{bucket}/warehouse',
            'io-impl'      = 'org.apache.iceberg.aws.s3.S3FileIO',
            'aws.region'   = '{region}'
        )
    """)
    logger.info("Glue catalog registered. warehouse=s3://%s/warehouse", bucket)


# ────────────────────────────────────────────────
# Kinesis Source 테이블
# ────────────────────────────────────────────────
def create_kinesis_source(t_env: StreamTableEnvironment, props: dict):
    stream = props["kinesis.stream"]
    region = props["aws.region"]

    t_env.execute_sql(f"""
        CREATE TABLE kinesis_opensky (
            icao24          STRING,
            callsign        STRING,
            origin_country  STRING,
            time_position   BIGINT,
            last_contact    BIGINT,
            longitude       DOUBLE,
            latitude        DOUBLE,
            baro_altitude   DOUBLE,
            on_ground       BOOLEAN,
            velocity        DOUBLE,
            true_track      DOUBLE,
            vertical_rate   DOUBLE,
            sensors         STRING,
            geo_altitude    DOUBLE,
            squawk          STRING,
            spi             BOOLEAN,
            position_source INT,
            category        INT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector'                = 'kinesis',
            'stream'                   = '{stream}',
            'aws.region'               = '{region}',
            'format'                   = 'json',
            'json.ignore-parse-errors' = 'true',
            'scan.stream.initpos'      = 'LATEST'
        )
    """)
    logger.info("Kinesis source table created: stream=%s", stream)


# ────────────────────────────────────────────────
# Iceberg Sink 테이블 (Glue Catalog)
# ────────────────────────────────────────────────
def create_iceberg_sink(t_env: StreamTableEnvironment, props: dict):
    database = props["glue.database"]
    table    = props["glue.table"]

    t_env.use_catalog("glue_catalog")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    t_env.use_database(database)

    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
            icao24          STRING,
            callsign        STRING,
            origin_country  STRING,
            time_position   BIGINT,
            last_contact    BIGINT,
            longitude       DOUBLE,
            latitude        DOUBLE,
            baro_altitude   DOUBLE,
            on_ground       BOOLEAN,
            velocity        DOUBLE,
            true_track      DOUBLE,
            vertical_rate   DOUBLE,
            sensors         STRING,
            geo_altitude    DOUBLE,
            squawk          STRING,
            spi             BOOLEAN,
            position_source INT,
            category        INT,
            ingest_ts       TIMESTAMP(3)
        ) PARTITIONED BY (origin_country)
        WITH (
            'format-version'               = '2',
            'write.upsert.enabled'         = 'false',
            'write.target-file-size-bytes' = '134217728'
        )
    """)
    logger.info("Iceberg sink table ready: glue_catalog.%s.%s", database, table)


# ────────────────────────────────────────────────
# 파이프라인 실행: Kinesis → Iceberg
# ────────────────────────────────────────────────
def run_pipeline(t_env: StreamTableEnvironment, props: dict):
    database = props["glue.database"]
    table    = props["glue.table"]

    t_env.execute_sql(f"""
        INSERT INTO glue_catalog.`{database}`.`{table}`
        SELECT
            icao24,
            callsign,
            origin_country,
            time_position,
            last_contact,
            longitude,
            latitude,
            baro_altitude,
            on_ground,
            velocity,
            true_track,
            vertical_rate,
            sensors,
            geo_altitude,
            squawk,
            spi,
            position_source,
            category,
            CAST(proc_time AS TIMESTAMP(3))
        FROM default_catalog.default_database.kinesis_opensky
    """)
    logger.info("Pipeline submitted.")


# ────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────
def main():
    props = get_app_properties()
    t_env = setup_environment(props)
    register_glue_catalog(t_env, props)
    create_kinesis_source(t_env, props)
    create_iceberg_sink(t_env, props)
    run_pipeline(t_env, props)


if __name__ == "__main__":
    main()
