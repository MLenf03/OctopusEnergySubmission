# Octopus Energy Pricing Analytics — Case Study (dbt + DuckDB)

> **Take-Home Challenge**: Price List Engine & Market Performance Analysis for an energy supplier entering the German PCW (Price Comparison Website) market.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Quick Start](#3-quick-start)
4. [Data Sources](#4-data-sources)
5. [Project Planning & Design Decisions](#5-project-planning--design-decisions)
6. [Assumptions](#6-assumptions)
7. [dbt Model Architecture](#7-dbt-model-architecture)
8. [Pricing Formula](#8-pricing-formula)
9. [Outputs & Key Findings](#9-outputs--key-findings)

---

## 1. Project Overview

This project builds a **Price List Engine** and a **Market Competitiveness Analyser** for Octopus Energy Germany, using [dbt](https://www.getdbt.com/) as the transformation layer on top of [DuckDB](https://duckdb.org/).

**Two products are priced** across 10 German postcodes:

| Product Code | Product Name | Energy Type |
|---|---|---|
| `DEU-ELECTRICITY-SELECT-PLUS-SECURE-12` | Select+ Secure 12 Strom | ELECTRICITY |
| `DEU-GAS-SELECT-PLUS-12` | OctopusSelect+ 12 Gas | GAS |

Both products have a **12-month contract and a 12-month price guarantee**.

The final outputs are:
- `exports/price_lists.csv` — PCW-ready price list (both products combined)
- `exports/market_analysis_scenarios.csv` — per-postcode competitive ranking under three margin scenarios
- `exports/market_performance_summary.csv` — executive-level win-rate summary

---

## 2. Repository Structure

```
OctopusEnergy/
├── octopus_pricing.duckdb        # DuckDB database (built by dbt)
├── dbt_octopus/
│   ├── dbt_project.yml           # Project config & variable defaults (margins, bonus targets, reference levels)
│   ├── profiles.yml              # DuckDB connection profile
│   ├── exports.py                # Post-run Python script: writes CSV exports from DuckDB
│   ├── seeds/
│   │   ├── basecost.csv          # Network & regulatory costs per postcode
│   │   ├── product.csv           # Octopus tariff specs & procurement costs
│   │   ├── comparison.csv        # Competitor snapshot from Check24.de
│   │   └── schema.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_basecost.sql
│   │   │   ├── stg_product.sql
│   │   │   ├── stg_comparison.sql
│   │   │   ├── stg_consumption_levels.sql  # lookup: reference consumption levels
│   │   │   ├── stg_margin_scenarios.sql    # lookup: margin scenario definitions
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_price_components.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── price_list.sql
│   │       ├── market_analysis_scenarios.sql
│   │       ├── market_performance_summary.sql
│   │       └── schema.yml
│   ├── tests/
│   │   └── assert_no_negative_net_unit_rate.sql
│   └── exports/
│       ├── price_lists.csv
│       ├── market_analysis_scenarios.csv
│       └── market_performance_summary.csv
```

---

## 3. Quick Start

### Open in Container

### Run the dbt project

```bash
cd dbt_octopus

# Build dbt project
dbt build
```

### Export the PCW file, scenarios & heatmaps

```bash
# 1. Export CSVs
python exports.py

# 2. Create visuals
python visualize.py
```

### Override margins:

#### At runtime (no code changes needed)

```bash
dbt run --vars '{"electricity_margin_ct_per_kwh": 1.0, "gas_margin_ct_per_kwh": 0.3}'
```
#### OR In the dbt_project file

---

## 4. Data Sources

### `basecost.csv` — Network & Regulatory Costs

### `product.csv` — Octopus Tariff Specifications

### `comparison.csv` — Competitor Snapshot (Check24.de)

---

## 5. Project Planning & Design Decisions

### Why dbt + DuckDB?

- **dbt** provides lineage, documentation, testability, and modular SQL — ideal for a pipeline that must be readable and extensible to more postcodes/tariffs.
- **DuckDB** runs locally without a server, supports full SQL, and handles the dataset with sub-second query times.
- Both are free, open-source, and require no infrastructure setup.

### Layered Architecture

The classic `staging → intermediate → marts` pattern:

| Layer | Purpose |
|---|---|
| **Staging** | Cast types, pad postcodes, collapse duplicate basecost bands, parse German date strings, Reusable lookup tables (consumption levels, margin scenarios) |
| **Intermediate** | core price computation (`int_price_components`) |
| **Marts** | Final outputs consumed by exports or analysts |

This means changing a margin assumption only requires editing `dbt_project.yml` (or passing `--vars`), and the change propagates through the entire pipeline. Audit ready with git versioning.

### Three-Scenario Strategy

Rather than producing a single price list, three margin scenarios are modelled in parallel:

| Scenario | Electricity Margin | Gas Margin | Purpose |
|---|---|---|---|
| **standard** | 1.5 ct/kWh | 0.5 ct/kWh | Live PCW submission |
| **aggressive** | 0.8 ct/kWh | 0.2 ct/kWh | Growth / volume positioning |
| **conservative** | 2.5 ct/kWh | 1.0 ct/kWh | Profitability analysis |

This enables the business to immediately model the revenue/rank trade-off without re-running dbt.

### Signup Bonus

- **Electricity**: €100 target; **Gas**: €140 target
- Capped at 15% of the annual gross price evaluated at the **lower boundary** of each PCW consumption band (`pcw_consumption_from`), this ensures the bonus is valid for the lowest-consuming customer in the band (requirement).

---

## 6. Assumptions

| # | Assumption | Impact |
|---|---|---|
| 1 | **VAT is 19%** for both electricity and gas (in `product.csv`). | If the product CSV changes, this updates automatically; no hard-coded VAT in SQL. |
| 2 | **Competitor standing charges are gross (incl. VAT)**, as shown on Check24. | Annual cost calculation: `unit_rate_ct/100 × kWh + standing_charge_annual − signup_bonus`. |
| 3 | **Annual margin estimate** = `margin × reference_kWh − (signup_bonus + lead_cost) / customer_lifetime_years`. | This is a simplified first-year economic model; it ignores upsell, and energy price risk. |
| 4 | **PCW consumption bands** are derived from four standard reference levels (1000 / 2000 / 4500 / 12000 kWh for electricity; 5000 / 12000 / 22000 / 40000 kWh for gas). | These match typical German household profiles (octopus website). |
| 5 | **Negative margins are illegal** (loss-leader pricing). A `GREATEST(margin, 0)` floor is applied. | This prevents the net unit rate from ever falling below `basecost + procurement`. |
| 6 | **Competitor contract length parsing**: German strings ("12 Monate", "1 Jahr", "2 Wochen") are converted to integer months via regex. | Weeks are rounded to months (7/30), which rounds short-term rolling contracts to 0. |

---

## 7. dbt Model Architecture

```
seeds/
  basecost  product  comparison
      │          │          │
  stg_basecost  stg_product  stg_comparison
                    │
         stg_consumption_levels  (staging)
         stg_margin_scenarios    (staging)
                    │
         int_price_components  ←── (basecost × product × scenarios × consumption levels)
                    │
          ┌─────────┴──────────────┐
       price_list        market_analysis_scenarios
                                   │
                        market_performance_summary
```

### Key model: `int_price_components`

This is the computational heart of the pipeline. For every combination of:
- Margin scenario (3)
- Octopus product (2)
- Postcode (10)
- Basecost consumption band (varies per postcode/energy type)
- Applicable reference consumption level (4 per energy type)

...it computes net/gross unit rates, monthly standing charges, the capped signup bonus, and annual costs.

**Grain**: `scenario × product_code × postcode × consumption_level_kwh`

---

## 8. Pricing Formula


```
net_unit_rate      = basecost_net_unit_rate + procurement_cost + max(margin, 0)
gross_unit_rate    = net_unit_rate × (1 + VAT)

gross_standing_charge_monthly = (basecost_net_standing_charge_annual + standing_charge_margin) × (1 + VAT) / 12

annual_gross_eur   = gross_unit_rate × consumption_kwh + gross_standing_charge_monthly × 12

bonus_cap_basis    = gross_unit_rate × pcw_consumption_from + gross_standing_charge_monthly × 12
signup_bonus       = min(target_bonus, bonus_cap_basis × 0.15)

annual_cost        = annual_gross_eur − signup_bonus

annual_margin_est  = margin × consumption_kwh − (signup_bonus + lead_cost) / customer_lifetime_years
```

`standing_charge_margin` is configured per energy type in `dbt_project.yml`: **10.0 EUR/year** (electricity) and **5.0 EUR/year** (gas).

---

## 9. Outputs & Key Findings

### Price Lists (`exports/price_lists.csv`)

- 80 rows: 10 postcodes × 4 consumption bands × 2 products
- Both ELECTRICITY and GAS in a single file
- Electricity gross unit rates range from **~25.1 ct/kWh** (Barßel) to **~33.4 ct/kWh** (Goch)
- Gas gross unit rates range from **~8.3 ct/kWh** (Barßel) to **~11.4 ct/kWh** (Braunschweig)

### Market Performance Summary — Electricity


| Scenario | Consumption | Avg Rank | % Cheapest | % Top-3 |
|---|---|---|---|---|
| Standard | low (1000 kWh) | 22.8 | 0% | 0% |
| Standard | medium (2000 kWh) | 10.4 | 0% | 10% |
| Standard | high (4500 kWh) | 15.3 | 0% | 10% |
| Standard | max (12000 kWh) | 1.0 | 100% | 100% |
| Aggressive | medium (2000 kWh) | 4.9 | 0% | 20% |

> **Note**: The 100% win rate at 12000 kWh is an artefact — no competitors in the snapshot bid at this consumption level.

**Headline finding**: Under the standard scenario, Octopus electricity is **not competitive at low consumption** (avg rank ~23 across 10 postcodes) but improves significantly at higher tiers. Dropping to the aggressive margin at medium consumption brings the average rank to ~5, placing Octopus in the top 3 for 20% of postcodes.
Signing bonus on 1000/2000 kwh capped due to PCW lower band submission , more granular submission could improve ranking.

### Market Performance Summary — Gas

> Sourced from `exports/market_performance_summary.csv`.

| Scenario | Consumption | Avg Rank | % Cheapest |
|---|---|---|---|
| Aggressive | low (5000 kWh) | 23.5 | 0% |
| Standard | medium (12000 kWh) | 24.5 | 0% |
| Conservative | high (22000 kWh) | 38.5 | 0% |

**Headline finding**: **Gas is consistently uncompetitive across all scenarios and all consumption levels.** Even the aggressive margin (0.2 ct/kWh) leaves Octopus Gas ranked 18–32 depending on tier, and never in the top 3. The procurement cost of 3.788 ct/kWh appears to be too high relative to what competitors are offering in the market.
