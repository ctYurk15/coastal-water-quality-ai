INSERT INTO timeseries_locations (timeseries_id, location_id)
SELECT 8, id
FROM locations
WHERE name LIKE 'IT%';