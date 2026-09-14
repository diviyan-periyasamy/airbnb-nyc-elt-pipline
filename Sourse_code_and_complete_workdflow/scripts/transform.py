"""
transform.py — TRANSFORM stage
Reads staging.cleaned_listings and builds the in-memory, analysis-ready
dataframe that the LOAD stage will write into the core star schema.

Implements the four required transformations:
    T1. Derived column  — price_category (Budget / Mid-range / Luxury)
    T2. Derived column  — availability_status (Rarely / Occasionally / Highly available)
    T3. Date formatting — parses last_review into full date parts and
                           computes days_since_last_review relative to a
                           fixed reference date (the day after the most
                           recent review in the dataset)
    T4. Aggregation      — neighbourhood-level summary (avg price,
                           avg availability, total estimated revenue)
Also computes estimated_annual_revenue = price * (365 - availability_365),
a standard derived business metric used in Airbnb market analyses
(estimated nights booked x nightly price).
"""

import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(__file__))
from db_utils import get_engine

REFERENCE_DATE = pd.Timestamp("2019-07-09")  # 1 day after the max last_review in the source data


def price_category(price: float) -> str:
    if price < 75:
        return "Budget"
    elif price < 200:
        return "Mid-range"
    else:
        return "Luxury"


def availability_status(days: int) -> str:
    if days < 30:
        return "Rarely Available"
    elif days < 200:
        return "Occasionally Available"
    else:
        return "Highly Available"


def transform():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM staging.cleaned_listings", engine)
    print(f"[transform] Transforming {len(df)} cleaned rows")

    # T1: derived column - price bucket
    df["price_category"] = df["price"].apply(price_category)

    # T2: derived column - availability bucket
    df["availability_status"] = df["availability_365"].apply(availability_status)

    # Derived business metric: estimated annual revenue
    df["estimated_annual_revenue"] = (df["price"] * (365 - df["availability_365"])).round(2)

    # T3: date formatting - parse last_review, derive days_since_last_review
    df["last_review"] = pd.to_datetime(df["last_review"], errors="coerce")
    df["days_since_last_review"] = (REFERENCE_DATE - df["last_review"]).dt.days
    df["days_since_last_review"] = df["days_since_last_review"].astype("Int64")  # nullable int

    print("[transform] Derived columns added: price_category, availability_status, "
          "estimated_annual_revenue, days_since_last_review")

    # T4: aggregation - neighbourhood-level summary
    agg = (
        df.groupby(["neighbourhood_group", "neighbourhood"])
        .agg(
            total_listings=("id", "count"),
            avg_price=("price", "mean"),
            avg_availability_365=("availability_365", "mean"),
            total_estimated_revenue=("estimated_annual_revenue", "sum"),
        )
        .reset_index()
    )
    agg["avg_price"] = agg["avg_price"].round(2)
    agg["avg_availability_365"] = agg["avg_availability_365"].round(2)
    agg["total_estimated_revenue"] = agg["total_estimated_revenue"].round(2)
    print(f"[transform] Built neighbourhood_summary aggregate with {len(agg)} rows")

    # Persist the transformed frames to disk (parquet) so the LOAD stage can
    # pick them up independently — mirrors how a real Airflow task hands off
    # data to the next task via a shared staging path / XCom reference.
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "_intermediate")
    os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(os.path.join(out_dir, "transformed_listings.parquet"), index=False)
    agg.to_parquet(os.path.join(out_dir, "neighbourhood_summary.parquet"), index=False)
    print(f"[transform] Wrote intermediate parquet files to {out_dir}")

    return len(df), len(agg)


if __name__ == "__main__":
    transform()
