{{
    config(
        engine='SummingMergeTree()',
        order_by='(approval_year, region, lending_instrument)'
    )
}}

SELECT
    toYear(board_approval_date)         AS approval_year,
    ifNull(region, '')                  AS region,
    ifNull(lending_instrument, '')      AS lending_instrument,
    count()                             AS project_count,
    sum(total_commitment_usd)           AS sum_commitment_usd,
    sum(ibrd_commitment_usd)            AS sum_ibrd_usd,
    sum(ida_commitment_usd)             AS sum_ida_usd
FROM {{ ref('stg_loans') }}
WHERE board_approval_date != toDate('1970-01-01')
  AND region != ''
GROUP BY approval_year, region, lending_instrument
