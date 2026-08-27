{{
    config(
        engine='ReplacingMergeTree(computed_at)',
        order_by='(region, country_code, lending_instrument, status)',
        partition_by='region'
    )
}}

SELECT
    ifNull(region, '')              AS region,
    ifNull(country_code, '')        AS country_code,
    ifNull(country, '')             AS country,
    ifNull(lending_instrument, '')  AS lending_instrument,
    ifNull(source, '')              AS source,
    ifNull(status, '')              AS status,
    count()                                             AS project_count,
    sum(total_commitment_usd)                           AS sum_commitment_usd,
    round(avg(total_commitment_usd), 2)                 AS avg_commitment_usd,
    sum(ibrd_commitment_usd)                            AS sum_ibrd_usd,
    sum(ida_commitment_usd)                             AS sum_ida_usd,
    sum(total_project_cost_usd)                         AS sum_project_cost_usd,
    countIf(status = 'active')                          AS active_count,
    countIf(status = 'closed')                          AS closed_count,
    now()                                               AS computed_at
FROM {{ ref('stg_loans') }}
WHERE region != ''
  AND country_code != ''
GROUP BY region, country_code, country, lending_instrument, source, status
