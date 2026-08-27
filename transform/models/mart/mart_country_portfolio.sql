{{
    config(
        engine='ReplacingMergeTree(computed_at)',
        order_by='(region, country_code)'
    )
}}

SELECT
    ifNull(region, '')          AS region,
    ifNull(country_code, '')    AS country_code,
    ifNull(country, '')         AS country,
    count()                                 AS total_projects,
    sum(total_commitment_usd)               AS sum_commitment_usd,
    sum(ibrd_commitment_usd)                AS sum_ibrd_usd,
    sum(ida_commitment_usd)                 AS sum_ida_usd,
    sum(total_project_cost_usd)             AS sum_project_cost_usd,
    countIf(status = 'active')              AS active_projects,
    countIf(status = 'closed')              AS closed_projects,
    min(board_approval_date)                AS first_approval_date,
    max(board_approval_date)                AS latest_approval_date,
    now()                                   AS computed_at
FROM {{ ref('stg_loans') }}
WHERE country_code != ''
GROUP BY region, country_code, country
