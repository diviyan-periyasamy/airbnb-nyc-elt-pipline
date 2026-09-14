"""
extract.py — EXTRACT stage
Reads the raw source CSV (AB_NYC_2019.csv, NYC Airbnb Open Data, 48,895 rows)
and loads it, completely unmodified, into staging.raw_listings.

This preserves an untouched copy of the source for auditing / re-processing,
following the same "extract into a staging area first" pattern covered in
the ETL pipeline walkthrough (raw CSV -> staging -> clean -> transform -> load).
"""

import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(__file__))
from db_utils import get_engine

DATA_PATH = os.getenv("AIRBNB_CSV_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "AB_NYC_2019.csv"))

RAW_COLUMNS = [
    "id", "name", "host_id", "host_name", "neighbourhood_group", "neighbourhood",
    "latitude", "longitude", "room_type", "price", "minimum_nights",
    "number_of_reviews", "last_review", "reviews_per_month",
    "calculated_host_listings_count", "availability_365",
]


def extract():
    print(f"[extract] Reading source CSV from {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)

    before = len(df)
    df = df[RAW_COLUMNS]
    print(f"[extract] Loaded {before} raw rows, {df.shape[1]} columns")

    engine = get_engine()
    df.to_sql(
        "raw_listings", engine, schema="staging",
        if_exists="append", index=False, method="multi", chunksize=5000,
    )
    print(f"[extract] Inserted {len(df)} rows into staging.raw_listings")
    return len(df)


if __name__ == "__main__":
    extract()
