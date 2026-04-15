{{
  config(materialized='view')
}}

/*
  Staging model for basecost seed.
  Casts postcode to VARCHAR (to preserve leading zeros) and ensures
  all numeric columns have the correct type.

  Some source rows split identical rate tiers into narrow 1 000 kWh bands
  (e.g. Freiberg GAS has ~40 consecutive rows all at the same unit rate and
  standing charge).  We collapse those into a single row per rate tier by
  grouping on (energy_type, postcode, town, rate, standing_charge) and
  taking MIN(consumption_from) / MAX(consumption_to) to get the true band
  boundaries.
*/

SELECT
    energy_type,
    LPAD(CAST(postcode AS VARCHAR), 5, '0')                   AS postcode,
    town,
    MIN(CAST(consumption_from AS INTEGER))                    AS consumption_from,
    MAX(CAST(consumption_to   AS INTEGER))                    AS consumption_to,
    CAST(basecost_net_unit_rate_eur_per_kwh   AS DOUBLE)      AS basecost_net_unit_rate_eur_per_kwh,
    CAST(basecost_net_standing_charge_eur_per_year AS DOUBLE) AS basecost_net_standing_charge_eur_per_year

FROM {{ ref('basecost') }}

GROUP BY
    energy_type,
    LPAD(CAST(postcode AS VARCHAR), 5, '0'),
    town,
    CAST(basecost_net_unit_rate_eur_per_kwh   AS DOUBLE),
    CAST(basecost_net_standing_charge_eur_per_year AS DOUBLE)
