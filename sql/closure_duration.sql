-- Eligibility and lifecycle are Silver rules, not reimplemented here.
-- Administrative closure is not satisfaction; still-open requests are excluded.
WITH valid_closures AS (
    SELECT
        agency,
        complaint_type,
        date_diff('microsecond', created_date_utc, closed_date_utc)
            / 3600000000.0 AS closure_hours
    FROM silver
    WHERE lifecycle_class = 'closed' AND duration_eligible
)
SELECT
    agency,
    complaint_type,
    count(*) AS valid_closed_count,
    quantile_cont(closure_hours, 0.5) AS median_closure_hours,
    quantile_cont(closure_hours, 0.9) AS p90_closure_hours
FROM valid_closures
GROUP BY agency, complaint_type
ORDER BY agency, complaint_type
