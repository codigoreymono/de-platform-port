-- Silver owns lifecycle classification; never infer pending from status != Closed.
WITH observed_population AS (
    SELECT
        agency,
        borough,
        lifecycle_class,
        date_diff('microsecond', created_date_utc, observed_at_utc) AS age_us
    FROM silver
    WHERE lifecycle_class IN ('outstanding', 'unknown')
), bands AS (
    SELECT
        agency,
        borough,
        lifecycle_class,
        CASE
            WHEN age_us IS NULL OR age_us < 0 THEN 'UNKNOWN'
            WHEN age_us < 604800000000 THEN '[0,7)'
            WHEN age_us < 1209600000000 THEN '[7,14)'
            WHEN age_us < 2592000000000 THEN '[14,30)'
            ELSE '30+'
        END AS age_band
    FROM observed_population
)
SELECT
    agency,
    borough,
    lifecycle_class,
    age_band,
    count(*) FILTER (WHERE lifecycle_class = 'outstanding') AS outstanding_count,
    count(*) FILTER (WHERE lifecycle_class = 'unknown') AS unknown_status_count
FROM bands
GROUP BY agency, borough, lifecycle_class, age_band
ORDER BY agency, borough, lifecycle_class, age_band
