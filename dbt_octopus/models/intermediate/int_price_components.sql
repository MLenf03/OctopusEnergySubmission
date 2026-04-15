{{
  config(materialized='view')
}}

/*
  Intermediate model: joins every product to every matching basecost row,
  every margin scenario, and every applicable consumption reference level,
  computing all price components — including the capped signup bonus — for
  all three margin scenarios (standard / aggressive / conservative).

  Pricing formula
  ---------------
  net_unit_rate = basecost_net_unit_rate + procurement_cost + effective_margin
  gross_unit_rate = net_unit_rate * (1 + VAT)
  gross_standing_charge_monthly = (basecost_net_standing_charge_annual + standing_charge_margin) * (1 + VAT) / 12
  annual_gross_eur = gross_unit_rate * consumption_level_kwh + gross_standing_charge_monthly * 12
  signup_bonus_eur = LEAST(target_signup_bonus_eur, annual_gross_eur * 0.15)
  annual_cost_after_bonus_eur = annual_gross_eur - signup_bonus_eur

  Margin floor (loss-limit guard)
  --------------------------------
  The effective margin is clamped to GREATEST(scenario_margin, 0.0) to prevent
  the net unit rate from falling below (basecost + procurement), which would
  constitute illegal loss-leader pricing.

  Signup bonus cap
  ----------------
  The target signup bonus (per energy type, configured via dbt vars) is capped
  at 15% of the annual gross price evaluated at the PCW band's lower boundary
  (pcw_consumption_from).  Using the lower boundary ensures the bonus is valid
  for the lowest consumer in the band, which is the regulatory requirement.

  pcw_consumption_from per band:
    lowest consumption tier  → basecost band's consumption_from
    all other tiers          → consumption_level_kwh (= previous tier's to + 1)

  Consumed by:
    • marts/price_list               (filters to scenario = 'standard')
    • marts/market_analysis_scenarios (uses all three scenarios)
*/

WITH products AS (
    SELECT * FROM {{ ref('stg_product') }}
),

basecosts AS (
    SELECT * FROM {{ ref('stg_basecost') }}
),

scenarios AS (
    SELECT * FROM {{ ref('stg_margin_scenarios') }}
),

consumption_levels AS (
    SELECT * FROM {{ ref('stg_consumption_levels') }}
),

-- Base: one row per (scenario × product × postcode × basecost band)
base AS (
    SELECT
        s.scenario,
        s.elec_margin_ct,
        s.gas_margin_ct,

        p.product_code,
        p.product_name,
        p.energy_type,
        b.postcode,
        b.town,
        b.consumption_from,
        b.consumption_to,

        -- Cost components (all net / excl. VAT)
        b.basecost_net_unit_rate_eur_per_kwh,
        p.procurement_cost_eur_per_kwh,
        -- Effective margin: scenario value clamped to 0 to prevent loss-leader pricing
        GREATEST(
            CASE
                WHEN p.energy_type = 'ELECTRICITY'
                    THEN s.elec_margin_ct / 100.0
                ELSE s.gas_margin_ct / 100.0
            END,
            0.0
        )                                              AS margin_eur_per_kwh,
        b.basecost_net_standing_charge_eur_per_year,

        p.vat_rate,
        p.lead_cost_eur,
        p.expected_customer_life_time_years,
        p.price_guarantee_months,
        p.contract_length_months,

        -- Derived: net unit rate (sum of all cost layers + effective margin)
        b.basecost_net_unit_rate_eur_per_kwh
            + p.procurement_cost_eur_per_kwh
            + GREATEST(
                CASE
                    WHEN p.energy_type = 'ELECTRICITY'
                        THEN s.elec_margin_ct / 100.0
                    ELSE s.gas_margin_ct / 100.0
                END,
                0.0
              )                                        AS net_unit_rate_eur_per_kwh,

        -- Derived: gross unit rate (incl. VAT)
        (
            b.basecost_net_unit_rate_eur_per_kwh
            + p.procurement_cost_eur_per_kwh
            + GREATEST(
                CASE
                    WHEN p.energy_type = 'ELECTRICITY'
                        THEN s.elec_margin_ct / 100.0
                    ELSE s.gas_margin_ct / 100.0
                END,
                0.0
              )
        ) * (1.0 + p.vat_rate)                        AS gross_unit_rate_eur_per_kwh,

        -- Gross standing charge (incl. standing charge margin and VAT)
        (
            b.basecost_net_standing_charge_eur_per_year
            + CASE
                WHEN p.energy_type = 'ELECTRICITY'
                    THEN CAST({{ var('electricity_standing_charge_margin_eur_per_year') }} AS DOUBLE)
                ELSE CAST({{ var('gas_standing_charge_margin_eur_per_year') }} AS DOUBLE)
            END
        ) * (1.0 + p.vat_rate) / 12.0                 AS gross_standing_charge_eur_per_month,

        -- Target signup bonus before cap (product-level, per energy type)
        CASE
            WHEN p.energy_type = 'ELECTRICITY'
                THEN CAST({{ var('electricity_signup_bonus_eur') }} AS DOUBLE)
            ELSE CAST({{ var('gas_signup_bonus_eur') }} AS DOUBLE)
        END                                            AS target_signup_bonus_eur

    FROM products p
    INNER JOIN basecosts b ON p.energy_type = b.energy_type
    CROSS JOIN scenarios s
),

-- Join with consumption reference levels; compute annual gross at reference consumption.
-- Grain: one row per (scenario × product × postcode × consumption_level).
with_consumption AS (
    SELECT
        b.*,
        cl.consumption_level_kwh,
        cl.consumption_label,

        -- Annual gross price at the reference consumption level
        b.gross_unit_rate_eur_per_kwh * cl.consumption_level_kwh
            + b.gross_standing_charge_eur_per_month * 12.0     AS annual_gross_eur

    FROM base b
    INNER JOIN consumption_levels cl
        ON  cl.energy_type           = b.energy_type
        AND cl.consumption_level_kwh BETWEEN b.consumption_from AND b.consumption_to
),

-- Compute pcw_consumption_from: the lower boundary of the PCW submission band.
-- The 15% bonus cap must be evaluated here (not at the reference midpoint) so that
-- the bonus is valid for the lowest consumer in the band (regulatory requirement).
--
-- Rule (within each scenario × product × postcode group):
--   lowest consumption tier → basecost band's consumption_from
--   all other tiers         → consumption_level_kwh (= previous tier's to + 1)
with_pcw_from AS (
    SELECT
        *,
        CASE
            WHEN consumption_level_kwh = MIN(consumption_level_kwh) OVER (
                PARTITION BY scenario, energy_type, postcode, product_code
            )
            THEN consumption_from
            ELSE consumption_level_kwh
        END                                                    AS pcw_consumption_from
    FROM with_consumption
),

-- Derive capped signup bonus using pcw_consumption_from.
with_bonus AS (
    SELECT
        *,
        -- Annual gross price evaluated at the PCW band's lower boundary
        gross_unit_rate_eur_per_kwh * pcw_consumption_from
            + gross_standing_charge_eur_per_month * 12.0       AS annual_gross_at_from_eur,

        -- Signup bonus capped at 15% of annual gross at the PCW band lower boundary
        LEAST(
            target_signup_bonus_eur,
            (gross_unit_rate_eur_per_kwh * pcw_consumption_from
                + gross_standing_charge_eur_per_month * 12.0) * 0.15
        )                                                      AS signup_bonus_eur

    FROM with_pcw_from
),

-- Annual cost after bonus — derived in a separate CTE so it can reference
-- signup_bonus_eur by name rather than duplicating the LEAST expression.
with_annual_cost AS (
    SELECT
        *,
        annual_gross_eur - signup_bonus_eur                    AS annual_cost_after_bonus_eur
    FROM with_bonus
)

SELECT * FROM with_annual_cost
