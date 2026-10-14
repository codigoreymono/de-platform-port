WITH channel_counts AS (
    SELECT
        created_date,
        borough,
        open_data_channel_type AS channel,
        count(*) AS request_count
    FROM silver
    GROUP BY created_date, borough, open_data_channel_type
)
SELECT
    created_date,
    borough,
    channel,
    request_count,
    request_count::DOUBLE / sum(request_count) OVER (
        PARTITION BY created_date, borough
    ) AS channel_share
FROM channel_counts
ORDER BY created_date, borough, channel
