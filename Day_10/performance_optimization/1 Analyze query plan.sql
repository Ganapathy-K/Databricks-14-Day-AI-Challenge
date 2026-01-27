%sql
EXPLAIN FORMATTED
SELECT brand, SUM(price) AS total_revenue
FROM delta.`dbfs:/Volumes/workspace/ecommerce/silver/events_delta`
WHERE date(event_time) = '2019-11-17'
GROUP BY brand;

