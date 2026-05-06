WITH source AS (
    SELECT * FROM {{ source('nigeria', 'raw_indicators') }}
),

cleaned AS (
    SELECT
        CAST(year AS INT64) AS year,
        country,
        country_code,
        ROUND(CAST(gdp_growth_pct AS FLOAT64), 2) AS gdp_growth_pct,
        ROUND(CAST(inflation_rate_pct AS FLOAT64), 2) AS inflation_rate_pct,
        ROUND(CAST(unemployment_rate_pct AS FLOAT64), 2) AS unemployment_rate_pct,
        ROUND(CAST(gdp_per_capita_usd AS FLOAT64), 2) AS gdp_per_capita_usd,
        ROUND(CAST(foreign_direct_investment_pct AS FLOAT64), 2) AS fdi_pct,
        economic_period,
        inflation_severity,
        ROUND(CAST(hardship_index AS FLOAT64), 2) AS hardship_index,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM source
    WHERE year IS NOT NULL
)

SELECT * FROM cleaned
ORDER BY year
