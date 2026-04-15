"""
visualize.py — Heatmap visualizations for Octopus market analysis scenarios.

"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

BASE_DIR = os.path.dirname(__file__)
EXPORTS = os.path.join(BASE_DIR, "exports")
VISUALIZATIONS_DIR = os.path.join(EXPORTS, "visualizations")

SCENARIOS = ["aggressive", "standard", "conservative"]
ENERGY_TYPES = ["ELECTRICITY", "GAS"]
COL_ORDER = ["low", "medium", "high", "max"]


def load():
    return pd.read_csv(os.path.join(EXPORTS, "market_analysis_scenarios.csv"))


def rank_heatmap(df, energy_type, scenario):
    os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)
    subset = df[(df["scenario"] == scenario) & (df["energy_type"] == energy_type)].copy()

    pivot = subset.pivot_table(
        index="postcode", columns="consumption_label", values="percentile_rank", aggfunc="mean"
    )
    col_order = [c for c in COL_ORDER if c in pivot.columns]
    pivot = pivot[col_order]

    fig, ax = plt.subplots(figsize=(7, 5))
    sns.heatmap(
        pivot,
        annot=True,
        fmt=".0f",
        cmap="RdYlGn_r",
        vmin=0,
        vmax=100,
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "Market Percentile Rank (lower = better)"},
    )
    ax.set_title(
        f"Octopus {energy_type.title()} — Market Percentile Rank\n({scenario.title()} Scenario, Postcode × Consumption)",
        fontsize=13,
        pad=12,
    )
    ax.set_xlabel("Consumption Level")
    ax.set_ylabel("Postcode")
    plt.tight_layout()

    filename = f"heatmap_{energy_type.lower()}_{scenario}.png"
    out = os.path.join(VISUALIZATIONS_DIR, filename)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def main():
    df = load()

    for energy_type in ENERGY_TYPES:
        for scenario in SCENARIOS:
            rank_heatmap(df, energy_type, scenario)


if __name__ == "__main__":
    main()
