/*
  stg_consumption_levels: standard consumption reference levels by energy type
  ===================================================================
  A single, reusable lookup table that maps each (energy_type, label) pair
  to the annual consumption level (kWh) used as a comparison benchmark in
  market-analysis models.

  Electricity and gas customers have very different typical consumption
  profiles, so each energy type has its own four reference points.

  Values are driven by dbt_project.yml vars so they can be overridden at
  runtime without touching model SQL:

    Electricity  — consumption_level_low/medium/high/max_kwh
    Gas          — gas_consumption_level_low/medium/high/max_kwh

  Consumed by:
    • intermediate/int_price_components
*/

SELECT 'ELECTRICITY' AS energy_type, CAST({{ var('consumption_level_low_kwh') }}          AS INTEGER) AS consumption_level_kwh, 'low'    AS consumption_label
UNION ALL
SELECT 'ELECTRICITY',                CAST({{ var('consumption_level_medium_kwh') }}        AS INTEGER),                          'medium'
UNION ALL
SELECT 'ELECTRICITY',                CAST({{ var('consumption_level_high_kwh') }}          AS INTEGER),                          'high'
UNION ALL
SELECT 'ELECTRICITY',                CAST({{ var('consumption_level_max_kwh') }}           AS INTEGER),                          'max'
UNION ALL
SELECT 'GAS',                        CAST({{ var('gas_consumption_level_low_kwh') }}       AS INTEGER),                          'low'
UNION ALL
SELECT 'GAS',                        CAST({{ var('gas_consumption_level_medium_kwh') }}    AS INTEGER),                          'medium'
UNION ALL
SELECT 'GAS',                        CAST({{ var('gas_consumption_level_high_kwh') }}      AS INTEGER),                          'high'
UNION ALL
SELECT 'GAS',                        CAST({{ var('gas_consumption_level_max_kwh') }}       AS INTEGER),                          'max'
