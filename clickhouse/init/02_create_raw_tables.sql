CREATE TABLE IF NOT EXISTS raw.kafka_loans (
    project_id              String,
    project_name            String,
    country                 String,
    country_code            String,
    region                  String,
    status                  String,
    lending_instrument      String,
    total_commitment_usd    String,
    ibrd_commitment_usd     String,
    ida_commitment_usd      String,
    total_project_cost_usd  String,
    board_approval_date     String,
    closing_date            String,
    approval_fy             String,
    source                  String,
    created_at              String,
    updated_at              String,
    __deleted               String,
    __op                    String,
    __ts_ms                 Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list           = 'kafka:9092',
    kafka_topic_list            = 'analytics.public.loans',
    kafka_group_name            = 'clickhouse_loans_consumer',
    kafka_format                = 'JSONEachRow',
    kafka_skip_broken_messages  = 5;


CREATE TABLE IF NOT EXISTS raw.loans (
    project_id              String,
    project_name            String,
    country                 String,
    country_code            String,
    region                  String,
    status                  String,
    lending_instrument      String,
    total_commitment_usd    Nullable(Decimal(20, 2)),
    ibrd_commitment_usd     Nullable(Decimal(20, 2)),
    ida_commitment_usd      Nullable(Decimal(20, 2)),
    total_project_cost_usd  Nullable(Decimal(20, 2)),
    board_approval_date     Nullable(Date),
    closing_date            Nullable(Date),
    approval_fy             Nullable(Int16),
    source                  String,
    created_at              DateTime,
    updated_at              DateTime,
    _deleted                UInt8 DEFAULT 0,
    _ingested_at            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(updated_at)
ORDER BY project_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_loans TO raw.loans AS
SELECT
    project_id,
    project_name,
    country,
    country_code,
    region,
    status,
    lending_instrument,
    toDecimal64OrNull(total_commitment_usd, 2)    AS total_commitment_usd,
    toDecimal64OrNull(ibrd_commitment_usd, 2)     AS ibrd_commitment_usd,
    toDecimal64OrNull(ida_commitment_usd, 2)      AS ida_commitment_usd,
    toDecimal64OrNull(total_project_cost_usd, 2)  AS total_project_cost_usd,
    toDateOrNull(board_approval_date)             AS board_approval_date,
    toDateOrNull(closing_date)                    AS closing_date,
    toInt16OrNull(approval_fy)                    AS approval_fy,
    source,
    parseDateTime64BestEffortOrZero(created_at)   AS created_at,
    parseDateTime64BestEffortOrZero(updated_at)   AS updated_at,
    if(__deleted = 'true', 1, 0)                  AS _deleted,
    now()                                         AS _ingested_at
FROM raw.kafka_loans;
