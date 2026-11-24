-- name: total_rows
SELECT COUNT(*) AS total_rows FROM earthquakes;

-- name: duplicate_ids (must return 0 rows)
SELECT event_id, COUNT(*) AS n FROM earthquakes GROUP BY event_id HAVING n > 1;

-- name: null_required_fields (must be 0)
SELECT COUNT(*) AS rows_with_nulls FROM earthquakes
WHERE event_time IS NULL OR magnitude IS NULL OR latitude IS NULL OR longitude IS NULL OR depth_km IS NULL;

-- name: date_range
SELECT MIN(event_time) AS first_event, MAX(event_time) AS last_event FROM earthquakes;

-- name: magnitude_stats
SELECT ROUND(MIN(magnitude), 2) AS min_mag, ROUND(AVG(magnitude), 2) AS avg_mag,
       ROUND(MAX(magnitude), 2) AS max_mag FROM earthquakes;

-- name: events_by_depth_class
SELECT depth_class, COUNT(*) AS events, ROUND(AVG(magnitude), 2) AS avg_mag
FROM earthquakes GROUP BY depth_class ORDER BY events DESC;

-- name: top_regions
SELECT region, COUNT(*) AS events, ROUND(MAX(magnitude), 1) AS max_mag
FROM earthquakes GROUP BY region ORDER BY events DESC LIMIT 10;

-- name: strongest_events
SELECT event_id, event_time, magnitude, depth_km, place
FROM earthquakes ORDER BY magnitude DESC LIMIT 5;

-- name: no_test_records (must be 0)
SELECT COUNT(*) AS test_rows FROM earthquakes WHERE event_id LIKE 'TEST_%';
