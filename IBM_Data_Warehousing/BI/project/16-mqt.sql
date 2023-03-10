CREATE MATERIALIZED VIEW max_waste_stats (city, stationid, trucktype, maxwastecollected) AS
(SELECT city, public."FactTrips".stationid, trucktype, MAX(wastecollected)
FROM public."FactTrips"
LEFT JOIN public."MyDimDate"
ON public."FactTrips".dateid = public."MyDimDate".dateid
LEFT JOIN public."MyDimStation"
ON public."FactTrips".stationid = public."MyDimStation".stationid
LEFT JOIN public."DimTruck"
ON public."FactTrips".truckid = public."DimTruck".truckid
GROUP BY city, public."FactTrips".stationid, trucktype);

