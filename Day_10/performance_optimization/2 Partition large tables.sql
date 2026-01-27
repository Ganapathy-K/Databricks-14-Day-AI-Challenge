%sql
CREATE TABLE ecommerce_partitioned USING DELTA PARTITIONED BY (event_date)
AS
SELECT *, date(event_time) AS event_date FROM delta.`dbfs:/Volumes/workspace/ecommerce/silver/events_delta`;
