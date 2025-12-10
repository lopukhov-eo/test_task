SELECT
    phrase,
    arraySort(x -> x.1, arrayAgg((toHour(event_time), sum_views))) AS views_by_hour
FROM
(
    SELECT
        phrase,
        toHour(event_time) AS hour,
        sum(views) AS sum_views
    FROM ads_table
    WHERE campaign_id = {campaign_id}
      AND toDate(event_time) = today()
    GROUP BY phrase, hour
)
GROUP BY phrase
ORDER BY phrase;
