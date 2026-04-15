{{
  config(materialized='table')
}}

/*
  Market Performance Summary
  ==========================
  Rolls up market_analysis_scenarios across all three margin scenarios
  (standard / aggressive / conservative) to produce a concise executive
  summary of Octopus Energy's competitive position.

  Rows are broken down by (scenario × energy_type × product × consumption_label)
  so that win-rates can be compared across scenarios and the four standard
  consumption levels:
    electricity: low (1 000 kWh), medium (2 000 kWh), high (4 500 kWh), max (12 000 kWh)
    gas:         low (5 000 kWh), medium (12 000 kWh), high (22 000 kWh), max (40 000 kWh)
*/

WITH base AS (
    SELECT *
    FROM {{ ref('fct_market_analysis_scenarios') }}
)

-- Win-rate summary per scenario, energy type, product, consumption level and contract length
SELECT
    scenario,
    energy_type,
    product,
    consumption_label,
    consumption_level_kwh,
    contract_length_months,
    margin_ct_per_kwh,
    COUNT(*)                                                  AS total_market_segments,
    SUM(CASE WHEN is_cheapest  THEN 1 ELSE 0 END)            AS segments_where_cheapest,
    SUM(CASE WHEN is_top_3     THEN 1 ELSE 0 END)            AS segments_where_top_3,
    ROUND(100.0 * SUM(CASE WHEN is_cheapest THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                             AS pct_cheapest,
    ROUND(100.0 * SUM(CASE WHEN is_top_3    THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                             AS pct_top_3,
    ROUND(AVG(our_rank), 1)                                  AS avg_rank,
    ROUND(AVG(percentile_rank), 1)                           AS avg_percentile,
    ROUND(AVG(delta_vs_cheapest_competitor_eur), 2)          AS avg_delta_vs_cheapest_eur,
    ROUND(AVG(our_annual_cost_after_bonus_eur), 2)           AS avg_our_annual_cost_eur,
    ROUND(AVG(avg_competitor_annual_cost_eur), 2)            AS avg_market_annual_cost_eur
FROM base
GROUP BY scenario, energy_type, product, consumption_label, consumption_level_kwh, contract_length_months, margin_ct_per_kwh
ORDER BY energy_type, product, consumption_level_kwh, contract_length_months, scenario
