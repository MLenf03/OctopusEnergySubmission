/*
  Singular test: asserts that the margin floor guard is working correctly.

  Returns rows where the net unit rate is negative (below total cost), which
  would indicate illegal loss-leader pricing.  An empty result = test passes.

  This can only fail if min_margin_ct_per_kwh is set to a value so negative
  that it drives net_unit_rate below zero — the GREATEST() clamp in
  int_price_components should prevent this entirely.
*/

SELECT
    product_code,
    product_name,
    energy_type,
    postcode,
    consumption_from,
    consumption_to,
    net_unit_rate_eur_per_kwh,
    margin_eur_per_kwh
FROM {{ ref('int_price_components') }}
WHERE net_unit_rate_eur_per_kwh < 0
   OR margin_eur_per_kwh < 0
