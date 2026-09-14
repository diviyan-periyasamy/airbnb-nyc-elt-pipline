-- =============================================================================
-- Coursework 1: Data Engineering
-- Analytical SQL Queries — business insights over the NYC Airbnb star schema
-- =============================================================================

-- Q1. Which borough (neighbourhood_group) generates the most estimated
--     annual revenue, and how does that compare to its listing count?
SELECT
    n.neighbourhood_group,
    COUNT(*)                                   AS total_listings,
    ROUND(AVG(f.price), 2)                     AS avg_price,
    ROUND(SUM(f.estimated_annual_revenue), 2)  AS total_estimated_revenue
FROM core.fact_listings f
JOIN core.dim_neighbourhood n ON n.neighbourhood_key = f.neighbourhood_key
GROUP BY n.neighbourhood_group
ORDER BY total_estimated_revenue DESC;


-- Q2. Top 10 individual neighbourhoods by average nightly price
--     (minimum 20 listings, to avoid noise from very small neighbourhoods).
SELECT
    n.neighbourhood_group,
    n.neighbourhood,
    COUNT(*)                 AS total_listings,
    ROUND(AVG(f.price), 2)   AS avg_price
FROM core.fact_listings f
JOIN core.dim_neighbourhood n ON n.neighbourhood_key = f.neighbourhood_key
GROUP BY n.neighbourhood_group, n.neighbourhood
HAVING COUNT(*) >= 20
ORDER BY avg_price DESC
LIMIT 10;


-- Q3. How does room type affect price and availability across the city?
SELECT
    r.room_type,
    COUNT(*)                          AS total_listings,
    ROUND(AVG(f.price), 2)            AS avg_price,
    ROUND(AVG(f.availability_365), 1) AS avg_availability_365,
    ROUND(AVG(f.number_of_reviews), 1) AS avg_number_of_reviews
FROM core.fact_listings f
JOIN core.dim_room_type r ON r.room_type_key = f.room_type_key
GROUP BY r.room_type
ORDER BY avg_price DESC;


-- Q4. Who are the top 10 hosts by total estimated annual revenue across
--     all of their listings? (identifies "power hosts" running multiple units)
SELECT
    h.host_id,
    h.host_name,
    h.host_listings_count,
    COUNT(f.listing_id)                        AS listings_in_data,
    ROUND(SUM(f.estimated_annual_revenue), 2)   AS total_estimated_revenue
FROM core.fact_listings f
JOIN core.dim_host h ON h.host_key = f.host_key
GROUP BY h.host_id, h.host_name, h.host_listings_count
ORDER BY total_estimated_revenue DESC
LIMIT 10;


-- Q5. Review recency and engagement: how many listings in each price
--     category have not been reviewed in over a year (i.e. are "stale")
--     versus reviewed recently? Highlights potential inactive listings.
SELECT
    f.price_category,
    COUNT(*) FILTER (WHERE f.days_since_last_review IS NULL)          AS never_reviewed,
    COUNT(*) FILTER (WHERE f.days_since_last_review > 365)            AS stale_over_1yr,
    COUNT(*) FILTER (WHERE f.days_since_last_review <= 365)           AS reviewed_within_1yr,
    COUNT(*)                                                          AS total_listings
FROM core.fact_listings f
GROUP BY f.price_category
ORDER BY total_listings DESC;


-- Q6. (Bonus) Using the pre-aggregated marts table: the 5 neighbourhoods
--     with the highest average availability (potential oversupply / low demand).
SELECT
    neighbourhood_group,
    neighbourhood,
    total_listings,
    avg_price,
    avg_availability_365
FROM marts.neighbourhood_summary
WHERE total_listings >= 10
ORDER BY avg_availability_365 DESC
LIMIT 5;
