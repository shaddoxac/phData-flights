select airlines.AIRLINE as AIRLINE, airports.AIRPORT as ORIGIN_AIRPORT, YEAR, MONTH, COUNT from 
(select AIRLINE, ORIGIN_AIRPORT, YEAR, MONTH, COUNT(*) as COUNT from public.flights group by AIRLINE, ORIGIN_AIRPORT, YEAR, MONTH) as flights
inner join public.airports as airports on airports.IATA_CODE = flights.ORIGIN_AIRPORT
inner join public.airlines as airlines on airlines.IATA_CODE = flights.AIRLINE;

select * from "USER_SHADDOXAC"."PUBLIC"."FLIGHTS_BY_AIRLINE_AIRPORT_MONTHLY";


select DEPARTURE_DELAY, ARRIVAL_DELAY from public.flights;

select airlines.AIRLINE as Airline, TotalDelayCount, TotalFlightCount, TotalDelayCount / TotalFlightCount as DelayPercentage from 
(select AIRLINE, SUM(case when (IFNULL(DEPARTURE_DELAY, 0) > 0) then 1 else (case when (IFNULL(ARRIVAL_DELAY, 0) > 0) then 1 else 0 end) end) as TotalDelayCount, COUNT(*) as TotalFlightCount from public.flights group by AIRLINE) as flights
inner join public.airlines as airlines on airlines.IATA_CODE = flights.AIRLINE;

select * from "USER_SHADDOXAC"."PUBLIC"."ON_TIME_PERCENTAGE_PER_AIRLINE";


select airlines.AIRLINE as Airline, NumDelays from
(select AIRLINE, SUM(case when (IFNULL(DEPARTURE_DELAY, 0) > 0) then 1 else (case when (IFNULL(ARRIVAL_DELAY, 0) > 0) then 1 else 0 end) end) as NumDelays from public.flights group by AIRLINE) as flights
inner join public.airlines as airlines on airlines.IATA_CODE = flights.AIRLINE
order by numdelays desc;


select * from "USER_SHADDOXAC"."PUBLIC"."AIRLINES_MOST_DELAYS";



select airports.AIRPORT as Airport, TotalCancellations, AirlineCarrierCancellations, WeatherCancellations, NationalAirSystemCancellations, SecurityCancellations from
(select ORIGIN_AIRPORT, 
        SUM(case when CANCELLATION_REASON != ''  then 1 else 0 end) as TotalCancellations,
        SUM(case when CANCELLATION_REASON  = 'A' then 1 else 0 end) as AirlineCarrierCancellations,
        SUM(case when CANCELLATION_REASON  = 'B' then 1 else 0 end) as WeatherCancellations,
        SUM(case when CANCELLATION_REASON  = 'C' then 1 else 0 end) as NationalAirSystemCancellations,
        SUM(case when CANCELLATION_REASON  = 'D' then 1 else 0 end) as SecurityCancellations
 from public.flights
 group by ORIGIN_AIRPORT
) as flights
inner join public.airports as airports on airports.IATA_CODE = flights.ORIGIN_AIRPORT;

select * from "USER_SHADDOXAC"."PUBLIC"."CANCELLATION_REASONS_BY_AIRPORT";


select airports.AIRPORT as Airport, AIR_SYSTEM_DELAY_COUNT, SECURITY_DELAY_COUNT, AIRLINE_DELAY_COUNT, LATE_AIRCRAFT_DELAY_COUNT, WEATHER_DELAY_COUNT from
(select ORIGIN_AIRPORT,
       SUM(AIR_SYSTEM_DELAY)    as AIR_SYSTEM_DELAY_COUNT,
       SUM(SECURITY_DELAY)      as SECURITY_DELAY_COUNT,
       SUM(AIRLINE_DELAY)       as AIRLINE_DELAY_COUNT,
       SUM(LATE_AIRCRAFT_DELAY) as LATE_AIRCRAFT_DELAY_COUNT,
       SUM(WEATHER_DELAY)       as WEATHER_DELAY_COUNT
 from public.flights
 group by ORIGIN_AIRPORT
) as flights
inner join public.airports as airports on airports.IATA_CODE = flights.ORIGIN_AIRPORT;

select * from "USER_SHADDOXAC"."PUBLIC"."DELAY_REASONS_BY_AIRPORT";



select airlines.AIRLINE as AIRLINE, UniqueRoutes from
(select AIRLINE, count(*) as UniqueRoutes from (select distinct AIRLINE, ORIGIN_AIRPORT, DESTINATION_AIRPORT from public.flights) as flights group by AIRLINE) as groupedFlights
inner join public.airlines as airlines on airlines.IATA_CODE = groupedFlights.AIRLINE
order by UniqueRoutes desc;

select * from "USER_SHADDOXAC"."PUBLIC"."AIRLINES_DISTINCT_ROUTES";