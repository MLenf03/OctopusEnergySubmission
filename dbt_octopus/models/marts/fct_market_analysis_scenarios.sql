{{
  config(materialized='table')
}}

/*
  Market Analysis — Scenario Comparison
  ======================================
  Shows how Octopus's competitive ranking changes across three margin scenarios,
  evaluated at the same four standard consumption levels used in market_analysis_scenarios.

  Scenarios
  ---------
  standard     — matches the margins used in the live price lists (PCW upload).
                 electricity: {{ var('electricity_margin_ct_per_kwh') }} ct/kWh
                 gas:         {{ var('gas_margin_ct_per_kwh') }} ct/kWh

  aggressive   — lower margins to maximise competitive positioning / volume growth.
                 electricity: {{ var('electricity_margin_aggressive_ct_per_kwh') }} ct/kWh
                 gas:         {{ var('gas_margin_aggressive_ct_per_kwh') }} ct/kWh

  conservative — higher margins for profitability / planning analysis.
                 electricity: {{ var('electricity_margin_conservative_ct_per_kwh') }} ct/kWh
                 gas:         {{ var('gas_margin_conservative_ct_per_kwh') }} ct/kWh

  For each scenario, Octopus prices are taken directly from int_price_components
  (which pre-computes all three scenarios).  Competitors are unchanged in all scenarios.

  Output columns
  --------------
  scenario                           — 'standard' | 'aggressive' | 'conservative'
  electricity_margin_ct_per_kwh      — Octopus electricity margin used in this scenario
  gas_margin_ct_per_kwh              — Octopus gas margin used in this scenario
  … same competitive metrics as market_performance_summary …
  delta_rank_vs_standard             — how many rank positions gained (−) or lost (+)
                                       vs. the standard scenario for the same segment
*/

WITH components AS (
    SELECT * FROM {{ ref('int_price_components') }}
),

competitors AS (
    SELECT * FROM {{ ref('stg_comparison') }}
),

-- Competitor ceiling-match: for each standard consumption level, pick the
-- competitor tier with the smallest quoted consumption >= that level.
-- Competitors are the same in every scenario — only Octopus prices change.
competitor_ceiling_match AS (
    SELECT
        c.energy_type,
        c.postcode,
        o.scenario,
        o.consumption_level_kwh                             AS std_consumption_level_kwh,
        o.consumption_label,
        c.provider,
        c.product_name,
        c.contract_length_months,
        MIN(c.consumption_level_kwh)                        AS matched_consumption_level_kwh

    FROM competitors c
    INNER JOIN (
        SELECT DISTINCT scenario, energy_type, postcode, consumption_level_kwh,
                        consumption_label, contract_length_months
        FROM components
    ) o
        ON  c.energy_type            = o.energy_type
        AND c.postcode               = o.postcode
        AND c.contract_length_months = o.contract_length_months
        AND c.consumption_level_kwh  >= o.consumption_level_kwh

    GROUP BY
        c.energy_type, c.postcode, o.scenario, o.consumption_level_kwh, o.consumption_label,
        c.provider, c.product_name, c.contract_length_months
),

competitor_at_consumption AS (
    SELECT
        m.scenario,
        m.energy_type,
        m.postcode,
        m.std_consumption_level_kwh                         AS consumption_level_kwh,
        m.consumption_label,
        c.contract_length_months,
        c.provider,
        c.product_name,
        c.unit_rate_ct_per_kwh,
        ROUND(c.standing_charge_eur_per_year, 2)            AS standing_charge_eur_per_year,
        c.signup_bonus_eur,
        c.unit_rate_ct_per_kwh / 100.0 * m.std_consumption_level_kwh
            + c.standing_charge_eur_per_year
            - c.signup_bonus_eur                            AS annual_cost_after_bonus_eur

    FROM competitor_ceiling_match m
    INNER JOIN competitors c
        ON  c.energy_type            = m.energy_type
        AND c.postcode               = m.postcode
        AND c.provider               = m.provider
        AND c.product_name           = m.product_name
        AND c.contract_length_months = m.contract_length_months
        AND c.consumption_level_kwh  = m.matched_consumption_level_kwh
),

-- All offers per scenario
all_offers AS (
    SELECT
        o.scenario,
        o.elec_margin_ct,
        o.gas_margin_ct,
        o.energy_type,
        o.postcode,
        o.consumption_level_kwh,
        o.consumption_label,
        o.contract_length_months,
        'Octopus Energy'                                    AS provider,
        o.product_name                                      AS product,
        o.gross_unit_rate_eur_per_kwh * 100.0               AS unit_rate_ct_per_kwh,
        ROUND(o.gross_standing_charge_eur_per_month * 12.0, 2) AS standing_charge_eur_per_year,
        o.signup_bonus_eur,
        o.annual_cost_after_bonus_eur,
        TRUE                                                AS is_octopus
    FROM components o

    UNION ALL

    SELECT
        c.scenario,
        NULL                                                AS elec_margin_ct,
        NULL                                                AS gas_margin_ct,
        c.energy_type,
        c.postcode,
        c.consumption_level_kwh,
        c.consumption_label,
        c.contract_length_months,
        c.provider,
        c.product_name                                      AS product,
        c.unit_rate_ct_per_kwh,
        c.standing_charge_eur_per_year,
        c.signup_bonus_eur,
        c.annual_cost_after_bonus_eur,
        FALSE                                               AS is_octopus
    FROM competitor_at_consumption c
),

-- Rank all offers within each (scenario × segment)
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY scenario, energy_type, postcode, consumption_level_kwh, contract_length_months
            ORDER BY annual_cost_after_bonus_eur ASC
        )                                                   AS cost_rank,
        COUNT(*) OVER (
            PARTITION BY scenario, energy_type, postcode, consumption_level_kwh, contract_length_months
        )                                                   AS total_offers_in_market
    FROM all_offers
),

-- Octopus rows with competitive context metrics
octopus_ranked AS (
    SELECT
        r.scenario,
        CASE
            WHEN r.energy_type = 'ELECTRICITY' THEN r.elec_margin_ct
            ELSE r.gas_margin_ct
        END                                                 AS margin_ct_per_kwh,
        r.energy_type,
        r.postcode,
        r.consumption_label,
        r.consumption_level_kwh,
        r.contract_length_months,
        r.provider,
        r.product,
        r.unit_rate_ct_per_kwh                              AS our_unit_rate_ct_per_kwh,
        r.standing_charge_eur_per_year                      AS our_standing_charge_eur_per_year,
        r.signup_bonus_eur                                  AS our_signup_bonus_eur,
        r.annual_cost_after_bonus_eur                       AS our_annual_cost_after_bonus_eur,
        r.cost_rank                                         AS our_rank,
        r.total_offers_in_market                            AS total_offers,
        r.cost_rank - 1                                     AS cheaper_offers_count,
        ROUND(100.0 * (r.cost_rank - 1) / NULLIF(r.total_offers_in_market - 1, 0), 1)
                                                            AS percentile_rank,
        r.cost_rank = 1                                     AS is_cheapest,
        r.cost_rank <= 3                                    AS is_top_3,

        -- Market averages for context (excl. Octopus)
        (SELECT ROUND(AVG(a.unit_rate_ct_per_kwh), 2)
         FROM ranked a
         WHERE a.scenario              = r.scenario
           AND a.energy_type           = r.energy_type
           AND a.postcode              = r.postcode
           AND a.consumption_level_kwh = r.consumption_level_kwh
           AND a.contract_length_months = r.contract_length_months
           AND NOT a.is_octopus)                            AS avg_competitor_unit_rate_ct,

        (SELECT ROUND(AVG(a.annual_cost_after_bonus_eur), 2)
         FROM ranked a
         WHERE a.scenario              = r.scenario
           AND a.energy_type           = r.energy_type
           AND a.postcode              = r.postcode
           AND a.consumption_level_kwh = r.consumption_level_kwh
           AND a.contract_length_months = r.contract_length_months
           AND NOT a.is_octopus)                            AS avg_competitor_annual_cost_eur,

        (SELECT ROUND(MIN(a.annual_cost_after_bonus_eur), 2)
         FROM ranked a
         WHERE a.scenario              = r.scenario
           AND a.energy_type           = r.energy_type
           AND a.postcode              = r.postcode
           AND a.consumption_level_kwh = r.consumption_level_kwh
           AND a.contract_length_months = r.contract_length_months
           AND NOT a.is_octopus)                            AS min_competitor_annual_cost_eur,

        ROUND(
            r.annual_cost_after_bonus_eur
            - (SELECT MIN(a.annual_cost_after_bonus_eur)
               FROM ranked a
               WHERE a.scenario              = r.scenario
                 AND a.energy_type           = r.energy_type
                 AND a.postcode              = r.postcode
                 AND a.consumption_level_kwh = r.consumption_level_kwh
                 AND a.contract_length_months = r.contract_length_months
                 AND NOT a.is_octopus),
            2
        )                                                   AS delta_vs_cheapest_competitor_eur

    FROM ranked r
    WHERE r.is_octopus = TRUE
),

-- Attach the standard-scenario rank so we can show rank movement per scenario
standard_ranks AS (
    SELECT
        energy_type,
        product,
        postcode,
        consumption_level_kwh,
        contract_length_months,
        cost_rank                                           AS standard_rank
    FROM ranked
    WHERE scenario = 'standard'
      AND is_octopus = TRUE
)

SELECT
    o.*,
    o.our_rank - s.standard_rank                           AS delta_rank_vs_standard
FROM octopus_ranked o
LEFT JOIN standard_ranks s
    ON  o.energy_type           = s.energy_type
    AND o.product               = s.product
    AND o.postcode              = s.postcode
    AND o.consumption_level_kwh = s.consumption_level_kwh
    AND o.contract_length_months = s.contract_length_months
ORDER BY o.energy_type, o.postcode, o.consumption_level_kwh, o.scenario
