import duckdb
con = duckdb.connect("octopus_pricing.duckdb")
con.execute(f"""
    COPY (
        SELECT energy_type, product_name, postcode, town,
                consumption_from, consumption_to,
                unit_rate_gross_ct_per_kwh,
                standing_charge_gross_eur_per_month,
                signup_bonus, contract_length, price_guarantee
        FROM fct_price_list
        ORDER BY energy_type, product_name, postcode, consumption_from
    ) TO 'exports/price_lists.csv' (HEADER, DELIMITER ',')
""")
print(f"Exported exports/price_lists.csv")
con.execute(f"""
    COPY (
        SELECT * FROM fct_market_analysis_scenarios
        ORDER BY energy_type, product, postcode, consumption_level_kwh
    ) TO 'exports/market_analysis_scenarios.csv' (HEADER, DELIMITER ',')
""")
print(f"Exported exports/market_analysis_scenarios.csv")
con.execute(f"""
    COPY (
        SELECT * FROM fct_market_performance_summary
    ) TO 'exports/market_performance_summary.csv' (HEADER, DELIMITER ',')
""")
print(f"Exported exports/market_performance_summary.csv")