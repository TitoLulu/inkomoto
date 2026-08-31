{{
    config(
        engine='ReplacingMergeTree(updated_at)',
        order_by='project_id',
        partition_by='toYear(board_approval_date)'
    )
}}

SELECT
    project_id,
    ifNull(project_name, '')                        AS project_name,
    ifNull(country, '')                             AS country,
    ifNull(upper(country_code), '')                 AS country_code,
    ifNull(region, '')                              AS region,
    ifNull(lower(status), '')                       AS status,
    ifNull(lending_instrument, '')                  AS lending_instrument,
    ifNull(source, '')                              AS source,
    ifNull(total_commitment_usd, 0)                 AS total_commitment_usd,
    ifNull(ibrd_commitment_usd, 0)                  AS ibrd_commitment_usd,
    ifNull(ida_commitment_usd, 0)                   AS ida_commitment_usd,
    ifNull(total_project_cost_usd, 0)               AS total_project_cost_usd,
    ifNull(approval_fy, 0)                          AS approval_fy,
    ifNull(board_approval_date, toDate('1970-01-01'))   AS board_approval_date,
    closing_date,
    updated_at
FROM {{ source('raw', 'loans') }}
FINAL
WHERE _deleted = 0
  AND project_id != ''
  AND status != ''
