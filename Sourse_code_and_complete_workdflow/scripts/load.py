"""
load.py — LOAD stage
Reads the transformed parquet files produced by transform.py and loads them
into the normalized core star schema (core.dim_*, core.fact_listings) and
the marts.neighbourhood_summary aggregate table.

Split into two functions so they can run as two separate, dependent Airflow
tasks: load_dimensions() must complete before load_fact() (the fact table's
foreign keys reference the surrogate keys generated here).
"""

import os
import sys
import pandas as pd
import calendar

sys.path.append(os.path.dirname(__file__))
from db_utils import get_engine

INTERMEDIATE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "_intermediate")


def _load_df(engine, df, table, schema, if_exists="append"):
    with engine.begin() as conn:
        df.to_sql(table, conn, schema=schema, if_exists=if_exists, index=False, method="multi", chunksize=5000)


def load_dimensions():
    engine = get_engine()
    df = pd.read_parquet(os.path.join(INTERMEDIATE_DIR, "transformed_listings.parquet"))
    print(f"[load_dimensions] {len(df)} transformed rows available")

    # dim_host
    dim_host = (
        df.groupby("host_id")
        .agg(host_name=("host_name", "first"), host_listings_count=("calculated_host_listings_count", "max"))
        .reset_index()
    )
    _load_df(engine, dim_host, "dim_host", "core")
    print(f"[load_dimensions] Loaded {len(dim_host)} rows into core.dim_host")

    # dim_neighbourhood
    dim_nbhd = df[["neighbourhood", "neighbourhood_group"]].drop_duplicates().reset_index(drop=True)
    _load_df(engine, dim_nbhd, "dim_neighbourhood", "core")
    print(f"[load_dimensions] Loaded {len(dim_nbhd)} rows into core.dim_neighbourhood")

    # dim_room_type
    dim_room = pd.DataFrame({"room_type": df["room_type"].dropna().unique()})
    _load_df(engine, dim_room, "dim_room_type", "core")
    print(f"[load_dimensions] Loaded {len(dim_room)} rows into core.dim_room_type")

    # dim_date — one row per distinct last_review date present in the data
    dates = df["last_review"].dropna().dt.normalize().unique()
    dim_date = pd.DataFrame({"full_date": pd.to_datetime(dates)})
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month.apply(lambda m: calendar.month_name[m])
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["weekday_name"] = dim_date["full_date"].dt.day_name()
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    _load_df(engine, dim_date, "dim_date", "core")
    print(f"[load_dimensions] Loaded {len(dim_date)} rows into core.dim_date")

    return len(dim_host), len(dim_nbhd), len(dim_room), len(dim_date)


def load_fact():
    engine = get_engine()
    df = pd.read_parquet(os.path.join(INTERMEDIATE_DIR, "transformed_listings.parquet"))

    dim_host = pd.read_sql("SELECT host_key, host_id FROM core.dim_host", engine)
    dim_nbhd = pd.read_sql("SELECT neighbourhood_key, neighbourhood, neighbourhood_group FROM core.dim_neighbourhood", engine)
    dim_room = pd.read_sql("SELECT room_type_key, room_type FROM core.dim_room_type", engine)
    dim_date = pd.read_sql("SELECT date_key, full_date FROM core.dim_date", engine)
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"])

    df = df.merge(dim_host, on="host_id", how="left")
    df = df.merge(dim_nbhd, on=["neighbourhood", "neighbourhood_group"], how="left")
    df = df.merge(dim_room, on="room_type", how="left")
    df["last_review_norm"] = df["last_review"].dt.normalize()
    df = df.merge(dim_date, left_on="last_review_norm", right_on="full_date", how="left")

    fact = pd.DataFrame({
        "listing_id": df["id"],
        "listing_name": df["name"],
        "host_key": df["host_key"],
        "neighbourhood_key": df["neighbourhood_key"],
        "room_type_key": df["room_type_key"],
        "last_review_date_key": df["date_key"],
        "latitude": df["latitude"],
        "longitude": df["longitude"],
        "price": df["price"],
        "price_category": df["price_category"],
        "minimum_nights": df["minimum_nights"],
        "number_of_reviews": df["number_of_reviews"],
        "reviews_per_month": df["reviews_per_month"],
        "calculated_host_listings_count": df["calculated_host_listings_count"],
        "availability_365": df["availability_365"],
        "availability_status": df["availability_status"],
        "estimated_annual_revenue": df["estimated_annual_revenue"],
        "days_since_last_review": df["days_since_last_review"],
    })

    _load_df(engine, fact, "fact_listings", "core")
    print(f"[load_fact] Loaded {len(fact)} rows into core.fact_listings")
    return len(fact)


def load_marts():
    engine = get_engine()
    agg = pd.read_parquet(os.path.join(INTERMEDIATE_DIR, "neighbourhood_summary.parquet"))
    _load_df(engine, agg, "neighbourhood_summary", "marts")
    print(f"[load_marts] Loaded {len(agg)} rows into marts.neighbourhood_summary")
    return len(agg)


if __name__ == "__main__":
    load_dimensions()
    load_fact()
    load_marts()
