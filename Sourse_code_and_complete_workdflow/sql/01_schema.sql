-- Student Number: COHNDDS25.1F-024
-- Coursework 1: Data Engineering 
-- PostgreSQL Database Schema — NYC Airbnb Open Data (2019)

DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS core CASCADE;
DROP SCHEMA IF EXISTS marts CASCADE;

CREATE SCHEMA staging;
CREATE SCHEMA core;
CREATE SCHEMA marts;

-- -----------------------------------------------------------------------------
-- STAGING LAYER — raw ingest, one-to-one with the source CSV
-- -----------------------------------------------------------------------------
CREATE TABLE staging.raw_listings (
    id                              BIGINT,
    name                            TEXT,
    host_id                         BIGINT,
    host_name                       TEXT,
    neighbourhood_group             TEXT,
    neighbourhood                   TEXT,
    latitude                        DOUBLE PRECISION,
    longitude                       DOUBLE PRECISION,
    room_type                       TEXT,
    price                           NUMERIC,
    minimum_nights                  INTEGER,
    number_of_reviews               INTEGER,
    last_review                     TEXT,          -- kept as TEXT: raw source format, parsed in transform stage
    reviews_per_month               NUMERIC,
    calculated_host_listings_count  INTEGER,
    availability_365                INTEGER,
    _ingested_at                    TIMESTAMP DEFAULT NOW()
);

-- Cleaned / validated version of the raw data (still one row per listing,
-- but nulls handled, duplicates removed, invalid records dropped or flagged)
CREATE TABLE staging.cleaned_listings (
    id                              BIGINT PRIMARY KEY,
    name                            TEXT,
    host_id                         BIGINT NOT NULL,
    host_name                       TEXT NOT NULL,
    neighbourhood_group             TEXT NOT NULL,
    neighbourhood                   TEXT NOT NULL,
    latitude                        DOUBLE PRECISION NOT NULL,
    longitude                       DOUBLE PRECISION NOT NULL,
    room_type                       TEXT NOT NULL,
    price                           NUMERIC NOT NULL CHECK (price > 0),
    minimum_nights                  INTEGER NOT NULL CHECK (minimum_nights > 0),
    number_of_reviews               INTEGER NOT NULL DEFAULT 0,
    last_review                     DATE,
    reviews_per_month               NUMERIC NOT NULL DEFAULT 0,
    calculated_host_listings_count  INTEGER NOT NULL DEFAULT 1,
    availability_365                INTEGER NOT NULL DEFAULT 0,
    _cleaned_at                     TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- CORE LAYER — normalized star schema (fact + dimensions)
-- -----------------------------------------------------------------------------

-- DIM: Host
CREATE TABLE core.dim_host (
    host_key            SERIAL PRIMARY KEY,
    host_id             BIGINT UNIQUE NOT NULL,
    host_name           TEXT NOT NULL,
    host_listings_count INTEGER NOT NULL DEFAULT 1
);

-- DIM: Neighbourhood (normalizes neighbourhood_group -> neighbourhood hierarchy)
CREATE TABLE core.dim_neighbourhood (
    neighbourhood_key    SERIAL PRIMARY KEY,
    neighbourhood        TEXT NOT NULL,
    neighbourhood_group  TEXT NOT NULL,
    UNIQUE (neighbourhood, neighbourhood_group)
);

-- DIM: Room type
CREATE TABLE core.dim_room_type (
    room_type_key SERIAL PRIMARY KEY,
    room_type     TEXT UNIQUE NOT NULL
);

-- DIM: Date (for last_review); one row per calendar day observed in the data
CREATE TABLE core.dim_date (
    date_key     SERIAL PRIMARY KEY,
    full_date    DATE UNIQUE NOT NULL,
    year         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   TEXT NOT NULL,
    day          INTEGER NOT NULL,
    weekday_name TEXT NOT NULL,
    quarter      INTEGER NOT NULL
);

-- FACT: Listings — one row per Airbnb listing, foreign keys to every dimension
-- plus derived measures produced in the transformation stage.
CREATE TABLE core.fact_listings (
    listing_id                 BIGINT PRIMARY KEY,
    listing_name               TEXT,
    host_key                   INTEGER NOT NULL REFERENCES core.dim_host(host_key),
    neighbourhood_key          INTEGER NOT NULL REFERENCES core.dim_neighbourhood(neighbourhood_key),
    room_type_key              INTEGER NOT NULL REFERENCES core.dim_room_type(room_type_key),
    last_review_date_key       INTEGER REFERENCES core.dim_date(date_key), -- nullable: listing may never have been reviewed
    latitude                   DOUBLE PRECISION,
    longitude                  DOUBLE PRECISION,
    price                      NUMERIC NOT NULL,
    price_category             TEXT NOT NULL,        -- derived column (transformation 1)
    minimum_nights              INTEGER NOT NULL,
    number_of_reviews          INTEGER NOT NULL,
    reviews_per_month           NUMERIC NOT NULL,
    calculated_host_listings_count INTEGER NOT NULL,
    availability_365           INTEGER NOT NULL,
    availability_status        TEXT NOT NULL,         -- derived column (transformation 1b)
    estimated_annual_revenue   NUMERIC NOT NULL,       -- derived column (transformation 2)
    days_since_last_review     INTEGER                -- derived column (transformation 3, date-based)
);

CREATE INDEX idx_fact_listings_neighbourhood ON core.fact_listings(neighbourhood_key);
CREATE INDEX idx_fact_listings_host          ON core.fact_listings(host_key);
CREATE INDEX idx_fact_listings_room_type     ON core.fact_listings(room_type_key);

-- -----------------------------------------------------------------------------
-- MARTS LAYER — pre-aggregated tables (transformation 4: aggregation)
-- -----------------------------------------------------------------------------
CREATE TABLE marts.neighbourhood_summary (
    neighbourhood_group   TEXT NOT NULL,
    neighbourhood         TEXT NOT NULL,
    total_listings        INTEGER NOT NULL,
    avg_price             NUMERIC NOT NULL,
    avg_availability_365  NUMERIC NOT NULL,
    total_estimated_revenue NUMERIC NOT NULL,
    PRIMARY KEY (neighbourhood_group, neighbourhood)
);
