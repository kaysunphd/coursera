SELECT public."MyDimDate".year, city, public."FactTrips".stationid, AVG(wastecollected) as avgwastecollected
FROM public."FactTrips"
LEFT JOIN public."MyDimDate"
ON public."FactTrips".dateid = public."MyDimDate".dateid
LEFT JOIN public."MyDimStation"
ON public."FactTrips".stationid = public."MyDimStation".stationid
GROUP BY CUBE(public."MyDimDate".year, city, public."FactTrips".stationid)
ORDER BY public."MyDimDate".year, city, public."FactTrips".stationid