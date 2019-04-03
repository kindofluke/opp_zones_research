-- with all_hc_facilities as (
--     select name, city, state, zip, latitude, longitude, 'hospital' as type, geog from hospitals
--     union all
--     select name, city, state, zip, y, x, 'urgent_care' as type, geog from urgent_care_facilities
-- )
-- select 
-- count(*) as healthcare_facilities_in_zones,
-- (SELECT COUNT(*) from all_hc_facilities) as total_hc_facilities,
-- (count(*)*1.0)/(SELECT COUNT(*) from all_hc_facilities) as healthcare_facilities_in_zones_pct

-- from qual_opp_zones qoz
-- join geo_tract gt on qoz.tract_geoid = gt.geoid
-- join all_hc_facilities hc on ST_Intersects(hc.geog, gt.geom);

with all_hc_facilities as (
    select name, city, state, zip, latitude, longitude, 'hospital' as type, geog from hospitals
    union all
    select name, city, state, zip, y, x, 'urgent_care' as type, geog from urgent_care_facilities
)
select 
count(*) as healthcare_facilities_in_zones,
(SELECT COUNT(*) from all_hc_facilities) as total_hc_facilities,
(count(*)*1.0)/(SELECT COUNT(*) from all_hc_facilities) as healthcare_facilities_in_zones_pct

from qual_opp_zones qoz
join geo_tract gt on qoz.tract_geoid = gt.geoid
join all_hc_facilities hc on ST_Intersects(ST_Buffer(hc.geog, 800), gt.geom);