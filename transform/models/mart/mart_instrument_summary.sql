{{
    config(
        engine='SummingMergeTree(sum_commitment_usd, sum_ibrd_usd, sum_ida_usd, project_count, active_count, closed_count)',
        order_by='(lending_instrument, region)'
    )
}}

SELECT
    ifNull(lending_instrument, '')      AS lending_instrument,
    ifNull(region, '')                  AS region,
    count()                             AS project_count,
    sum(total_commitment_usd)           AS sum_commitment_usd,
    sum(ibrd_commitment_usd)            AS sum_ibrd_usd,
    sum(ida_commitment_usd)             AS sum_ida_usd,
    countIf(status = 'active')          AS active_count,
    countIf(status = 'closed')          AS closed_count,
    min(board_approval_date)            AS first_approval_date,
    max(board_approval_date)            AS latest_approval_date
FROM {{ ref('stg_loans') }}
WHERE lending_instrument != ''
  AND region != ''
GROUP BY lending_instrument, region
