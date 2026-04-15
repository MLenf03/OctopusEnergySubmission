{{
  config(materialized='table')
}}

/*
  Price List — PCW submission format
  ===================================
  Generates the final price list for submission to Price Comparison Websites
  (Check24, Verivox, etc.) for both the electricity and gas tariffs.

  Consumption band definition
  ---------------------------
  One row per (product × postcode × reference consumption level).  The
  consumption_from / consumption_to columns define contiguous, non-overlapping
  PCW bands derived from the standard reference levels:

    • The lowest tier's consumption_from = the basecost band's consumption_from
      (ensures the price list covers from the minimum basecost consumption).
    • All higher tiers' consumption_from = the reference level's kWh value
      (= previous tier's consumption_to + 1).
    • Non-max tiers' consumption_to = next reference level – 1.
    • The max tier's consumption_to = MAX(basecost consumption_to) in the group.

  The unit rate for each row is taken from the basecost band that contains the
  reference consumption level (not from the derived PCW band boundaries).

  Signup bonus logic
  ------------------
  A target signup bonus (EUR) is configured per energy type via dbt variables:
    electricity_signup_bonus_eur (default 100 EUR)
    gas_signup_bonus_eur         (default  50 EUR)

  The 15% cap is enforced at the PCW band's consumption_from: the annual gross
  price is evaluated at the lower boundary of each row's PCW consumption band
  so the bonus never exceeds 15% of the annual cost for the lowest consumer in
  that band.  This is the regulatory interpretation — the offering must be valid
  for the cheapest customer in the band.

  Only the standard margin scenario is included in this output (for PCW
  submission).  All three scenarios are used in the market analysis models
  (market_analysis_scenarios, market_performance_summary).

  Margin analysis columns (diagnostic, not sent to PCW)
  ------------------------------------------------------
  annual_margin_eur — estimated first-year margin per customer at the reference
  consumption level, after amortised lead and bonus costs.
*/

-- Filter to standard margin scenario only: PCW submission uses standard margins.
WITH components AS (
    SELECT * FROM {{ ref('int_price_components') }}
    WHERE scenario = 'standard'
),

-- Derive PCW consumption_to: the upper boundary of each band.
--
-- Rule (within each product × postcode group, ordered by consumption_level_kwh):
--   non-max tier → LEAD(consumption_level_kwh) – 1
--   max tier     → MAX(basecost consumption_to) across the group
with_pcw_bands AS (
    SELECT
        *,
        COALESCE(
            LEAD(consumption_level_kwh) OVER (
                PARTITION BY energy_type, postcode, product_code
                ORDER BY consumption_level_kwh
            ) - 1,
            MAX(consumption_to) OVER (
                PARTITION BY energy_type, postcode, product_code
            )
        )                                                   AS pcw_consumption_to,

        -- Estimated annual margin per customer at the reference consumption level:
        --   unit margin revenue (margin × reference consumption)
        --   minus amortised acquisition costs (lead + signup bonus / lifetime)
        (margin_eur_per_kwh * consumption_level_kwh)
        + CASE
            WHEN energy_type = 'ELECTRICITY'
                    THEN CAST({{ var('electricity_standing_charge_margin_eur_per_year') }} AS DOUBLE)
                ELSE CAST({{ var('gas_standing_charge_margin_eur_per_year') }} AS DOUBLE)
              END
            - (
                (signup_bonus_eur + lead_cost_eur)
                / NULLIF(expected_customer_life_time_years, 0)
              )                                             AS annual_margin_eur

    FROM components
)

SELECT
    -- Reference consumption point dimensions
    consumption_level_kwh,
    consumption_label,

    -- PCW submission columns
    energy_type,
    product_name,
    postcode,
    town,
    pcw_consumption_from                                   AS consumption_from,
    pcw_consumption_to                                     AS consumption_to,
    ROUND(gross_unit_rate_eur_per_kwh * 100.0, 4)         AS unit_rate_gross_ct_per_kwh,
    ROUND(gross_standing_charge_eur_per_month, 2)          AS standing_charge_gross_eur_per_month,
    ROUND(signup_bonus_eur, 2)                             AS signup_bonus,
    contract_length_months                                 AS contract_length,
    price_guarantee_months                                 AS price_guarantee,

    -- Diagnostic / margin analysis columns
    ROUND(net_unit_rate_eur_per_kwh * 100.0, 4)           AS unit_rate_net_ct_per_kwh,
    ROUND(basecost_net_unit_rate_eur_per_kwh * 100.0, 4)  AS basecost_unit_rate_ct_per_kwh,
    ROUND(procurement_cost_eur_per_kwh * 100.0, 4)        AS procurement_unit_rate_ct_per_kwh,
    ROUND(margin_eur_per_kwh * 100.0, 4)                  AS margin_ct_per_kwh,
    ROUND(annual_gross_eur, 2)                             AS annual_gross_eur,
    ROUND(annual_gross_at_from_eur, 2)                     AS annual_gross_at_from_eur,
    ROUND(annual_margin_eur, 2)                            AS annual_margin_eur

FROM with_pcw_bands
ORDER BY energy_type, product_name, postcode, consumption_level_kwh
