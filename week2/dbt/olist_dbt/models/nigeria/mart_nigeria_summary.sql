WITH base AS (
    SELECT * FROM {{ ref('stg_nigeria_economics') }}
)

SELECT
    year,
    country,
    gdp_growth_pct,
    inflation_rate_pct,
    unemployment_rate_pct,
    gdp_per_capita_usd,
    fdi_pct,
    hardship_index,
    economic_period,
    inflation_severity,
    -- Decade classification
    CASE
        WHEN year BETWEEN 2010 AND 2014 THEN 'Early 2010s (Pre-crisis)'
        WHEN year BETWEEN 2015 AND 2019 THEN 'Late 2010s (Oil crash era)'
        WHEN year BETWEEN 2020 AND 2024 THEN '2020s (COVID + Naira crisis)'
        ELSE 'Other'
    END AS era,
    -- Is it a crisis year?
    CASE
        WHEN gdp_growth_pct < 0 OR inflation_rate_pct > 25 THEN TRUE
        ELSE FALSE
    END AS is_crisis_year
FROM base
ORDER BY year
