/*
  stg_margin_scenarios: margin scenario definitions
  ==========================================
  A single, reusable lookup table that enumerates the three margin scenarios
  used in market-analysis models.  Values are driven by dbt_project.yml vars
  so they can be overridden at runtime without touching model SQL.

  Columns
  -------
  scenario          — 'standard' | 'aggressive' | 'conservative'
  elec_margin_ct    — Octopus electricity margin in ct/kWh for this scenario
  gas_margin_ct     — Octopus gas margin in ct/kWh for this scenario

  Consumed by:
    • intermediate/int_price_components
*/

SELECT
    'standard'                                                            AS scenario,
    CAST({{ var('electricity_margin_ct_per_kwh') }}              AS DOUBLE) AS elec_margin_ct,
    CAST({{ var('gas_margin_ct_per_kwh') }}                      AS DOUBLE) AS gas_margin_ct
UNION ALL
SELECT
    'aggressive',
    CAST({{ var('electricity_margin_aggressive_ct_per_kwh') }}   AS DOUBLE),
    CAST({{ var('gas_margin_aggressive_ct_per_kwh') }}           AS DOUBLE)
UNION ALL
SELECT
    'conservative',
    CAST({{ var('electricity_margin_conservative_ct_per_kwh') }} AS DOUBLE),
    CAST({{ var('gas_margin_conservative_ct_per_kwh') }}         AS DOUBLE)
