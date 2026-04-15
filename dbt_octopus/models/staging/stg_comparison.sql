{{
  config(materialized='view')
}}

/*
  Staging model for competitor comparison snapshot (Check24.de).
  Produces one row per (energy_type × postcode × provider × product ×
  consumption_level_kwh × contract_length), preserving the exact price that
  Check24 quotes at each consumption level.

  Why we keep consumption_level_kwh
  -----------------------------------
  On Check24, many providers quote different standing charges and signup bonuses
  at each consumption tier (the standing charge may be tiered, or the bonus may
  scale with usage).  Collapsing these into one row per product caused two
  problems:
    1. Multiple rows per product (one per distinct standing-charge/bonus
       combination) inflated total_offers in market_analysis.
    2. The standing charge applied in annual-cost calculations was arbitrary
       (whichever rate combination survived the GROUP BY), not the one actually
       quoted at the consumption level being evaluated.

  The fix is to keep consumption_level_kwh explicit and join on it in
  market_analysis.sql so that each competitor contributes exactly one offer
  per (postcode × consumption_level) market segment.

  How to compare competitors
  --------------------------
  1. By unit_rate_ct_per_kwh — dominant cost driver at high consumption.
  2. By annual_cost at each standard consumption level (done in
     market_analysis.sql):
       unit_rate / 100 * consumption + standing_charge - signup_bonus
  3. By break-even consumption — the usage level at which two products cost
     the same:
       break_even = (sc_A - sc_B + bonus_B - bonus_A) / ((ur_B - ur_A) / 100)
*/

-- Parse the German duration string into an integer number of months.
-- "1 Monat" / "12 Monate" → extract leading integer directly (already in months)
-- "1 Woche" / "2 Wochen"  → weeks × 7 / 30 (rounds to 0 for short-term rolling offers)
-- "1 Jahr"  / "2 Jahre"   → years × 12
WITH parsed AS (
    SELECT
        *,
        CASE
            WHEN LOWER(contract_length_months) LIKE '%woche%'   -- covers "Woche" and "Wochen"
                THEN CAST(ROUND(
                        CAST(REGEXP_EXTRACT(contract_length_months, '(\d+)', 1) AS INTEGER)
                        * 7.0 / 30.0   -- weeks-to-months: 7 days per week / 30 days per month
                    ) AS INTEGER)
            WHEN LOWER(contract_length_months) LIKE '%jahr%'    -- covers "Jahr" and "Jahre"
                THEN CAST(REGEXP_EXTRACT(contract_length_months, '(\d+)', 1) AS INTEGER)
                     * 12              -- years-to-months: 12 months per year
            WHEN LOWER(contract_length_months) LIKE '%monat%'   -- covers "Monat" and "Monate"
                THEN CAST(REGEXP_EXTRACT(contract_length_months, '(\d+)', 1) AS INTEGER)
            ELSE
                CAST(REGEXP_EXTRACT(contract_length_months, '(\d+)', 1) AS INTEGER)
        END AS contract_length_months_int
    FROM {{ ref('comparison') }}
)

SELECT
    energy_type,
    LPAD(CAST(postcode AS VARCHAR), 5, '0')         AS postcode,
    location_name,
    CAST(consumption_level_kwh AS INTEGER)           AS consumption_level_kwh,
    provider,
    product                                           AS product_name,
    CAST(unit_rate_ct_per_kwh AS DOUBLE)             AS unit_rate_ct_per_kwh,
    CAST(standing_charge_eur_per_year AS DOUBLE)     AS standing_charge_eur_per_year,
    CAST(standing_charge_eur_per_year AS DOUBLE) / 12.0 AS standing_charge_eur_per_month,
    CAST(signup_bonus AS DOUBLE)                     AS signup_bonus_eur,
    CAST(price_guarantee_months AS INTEGER)          AS price_guarantee_months,
    contract_length_months_int                       AS contract_length_months

FROM parsed

GROUP BY
    energy_type,
    LPAD(CAST(postcode AS VARCHAR), 5, '0'),
    location_name,
    CAST(consumption_level_kwh AS INTEGER),
    provider,
    product,
    CAST(unit_rate_ct_per_kwh AS DOUBLE),
    CAST(standing_charge_eur_per_year AS DOUBLE),
    CAST(signup_bonus AS DOUBLE),
    CAST(price_guarantee_months AS INTEGER),
    contract_length_months_int
