--QUESTION - 3: Counting short trips (ANSWER:8,007)
SELECT
  lpep_pickup_datetime,
  trip_distance
FROM green_tripdata_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1
ORDER BY trip_distance DESC;

--QUESTION - 4: Longest trip for each day (ANSWER: 2025-11-20)
SELECT 
	TO_CHAR(lpep_pickup_datetime, 'YYYY-MM-DD') AS pickup_date,
	ROUND(SUM(trip_distance)::numeric,2) as total_distance
FROM public.green_tripdata_2025_11
where trip_distance < 100
group by pickup_date
order by total_distance desc

--QUESTION - 5: Biggest pickup zone (ANSWER: East Harlem North)
SELECT 
	pu."Zone" as Pickup_Zone,
	ROUND(SUM(t.total_amount)::integer,2) as total_amount
FROM public.green_tripdata_2025_11 as t
LEFT JOIN public.taxi_zone_lookup as pu
  ON pu."LocationID" = t."PULocationID"
where TO_CHAR(lpep_pickup_datetime, 'YYYY-MM-DD') = '2025-11-18'
group by Pickup_Zone
order by total_amount desc

--QUESTION - 6: Largest tip (ANSWER: Yorkville West)
SELECT
  doo."Zone" AS dropoff_zone,
  t.tip_amount AS largest_tip
FROM green_tripdata_2025_11 t
JOIN taxi_zone_lookup pu
  ON pu."LocationID" = t."PULocationID"
JOIN taxi_zone_lookup doo
  ON doo."LocationID" = t."DOLocationID"
WHERE pu."Zone" = 'East Harlem North'
  AND t.tip_amount IS NOT NULL
ORDER BY t.tip_amount DESC
LIMIT 1;
