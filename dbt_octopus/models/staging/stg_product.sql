{{
  config(materialized='view')
}}

/*
  Staging model for the product seed.
  Converts procurement cost from ct/kWh to EUR/kWh for consistent
  calculation with basecost (which is already in EUR/kWh).
*/

SELECT
    product_code,
    product_name,
    energy_type,
    CAST(expected_customer_life_time AS DOUBLE)    AS expected_customer_life_time_years,
    CAST(lead_cost AS DOUBLE)                       AS lead_cost_eur,
    CAST(vat AS DOUBLE)                             AS vat_rate,
    CAST(net_procurement_cost_ct_per_kwh AS DOUBLE) AS procurement_cost_ct_per_kwh,
    -- convert to EUR/kWh for arithmetic with basecost columns
    CAST(net_procurement_cost_ct_per_kwh AS DOUBLE) / 100.0 AS procurement_cost_eur_per_kwh,
    CAST(price_guarantee_months AS INTEGER)         AS price_guarantee_months,
    CAST(contract_length_months AS INTEGER)         AS contract_length_months

FROM {{ ref('product') }}
