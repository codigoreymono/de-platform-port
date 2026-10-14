WITH category_counts AS (
    SELECT created_date, borough, complaint_type, count(*) AS request_count
    FROM silver
    GROUP BY created_date, borough, complaint_type
)
SELECT
    created_date,
    borough,
    complaint_type,
    request_count,
    request_count::DOUBLE / sum(request_count) OVER (
        PARTITION BY created_date, borough
    ) AS category_share
FROM category_counts
ORDER BY created_date, borough, complaint_type
