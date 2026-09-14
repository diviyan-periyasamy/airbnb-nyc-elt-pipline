"""
clean.py — CLEAN & VALIDATE stage
Reads staging.raw_listings and produces staging.cleaned_listings:
    * removes duplicate listing ids (keep first occurrence)
    * fills missing categorical values with explicit placeholders
    * fills missing review-related numeric fields for never-reviewed listings
    * parses last_review into a real DATE (or NULL)
    * drops invalid records: non-positive price, non-positive minimum_nights,
      and coordinates outside the New York City bounding box
Every rule below corresponds directly to a data-quality issue found during
profiling of the raw file (see Technical Report, Section 4).
"""

import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(__file__))
from db_utils import get_engine

# Approximate bounding box for New York City (used to reject bad GPS coordinates)
NYC_LAT_RANGE = (40.45, 40.95)
NYC_LON_RANGE = (-74.30, -73.65)


def clean():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM staging.raw_listings", engine)
    n_start = len(df)
    print(f"[clean] Starting with {n_start} raw rows")

    # 1. Remove duplicate listing ids
    dup_count = df["id"].duplicated().sum()
    df = df.drop_duplicates(subset="id", keep="first")
    print(f"[clean] Removed {dup_count} duplicate id rows")

    # 2. Handle missing categorical/text values
    missing_name = df["name"].isna().sum()
    df["name"] = df["name"].fillna("Unnamed Listing")

    missing_host = df["host_name"].isna().sum()
    df["host_name"] = df["host_name"].fillna("Unknown Host")
    print(f"[clean] Filled {missing_name} missing names, {missing_host} missing host names")

    # 3. Handle missing review data — NaN here always means "never reviewed"
    #    (number_of_reviews == 0 for every row where last_review is NaN)
    never_reviewed = df["last_review"].isna().sum()
    df["reviews_per_month"] = df["reviews_per_month"].fillna(0)
    df["last_review_parsed"] = pd.to_datetime(df["last_review"], errors="coerce")
    print(f"[clean] {never_reviewed} listings have never been reviewed (last_review left NULL)")

    # 4. Drop invalid records
    before = len(df)
    df = df[df["price"] > 0]
    invalid_price = before - len(df)

    before = len(df)
    df = df[df["minimum_nights"] > 0]
    invalid_min_nights = before - len(df)

    before = len(df)
    df = df[
        df["latitude"].between(*NYC_LAT_RANGE) & df["longitude"].between(*NYC_LON_RANGE)
    ]
    invalid_coords = before - len(df)

    print(f"[clean] Dropped {invalid_price} rows with price <= 0")
    print(f"[clean] Dropped {invalid_min_nights} rows with minimum_nights <= 0")
    print(f"[clean] Dropped {invalid_coords} rows with coordinates outside NYC bounding box")

    # 5. Final null-safety defaults for remaining numeric fields
    df["number_of_reviews"] = df["number_of_reviews"].fillna(0).astype(int)
    df["calculated_host_listings_count"] = df["calculated_host_listings_count"].fillna(1).astype(int)
    df["availability_365"] = df["availability_365"].fillna(0).astype(int)

    out = df[[
        "id", "name", "host_id", "host_name", "neighbourhood_group", "neighbourhood",
        "latitude", "longitude", "room_type", "price", "minimum_nights",
        "number_of_reviews", "last_review_parsed", "reviews_per_month",
        "calculated_host_listings_count", "availability_365",
    ]].rename(columns={"last_review_parsed": "last_review"})

    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE staging.cleaned_listings")
        out.to_sql("cleaned_listings", conn, schema="staging", if_exists="append", index=False, method="multi", chunksize=5000)

    print(f"[clean] Wrote {len(out)} cleaned rows to staging.cleaned_listings "
          f"({n_start - len(out)} rows removed in total)")
    return len(out)


if __name__ == "__main__":
    clean()
