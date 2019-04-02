drop table if exists adjacent_analysis;
create table adjacent_analysis as 
with qual_tracts as (
    SELECT 
        qoz.tract_geoid,
        geom,
        state,
        county_name,
        designation
        from qual_opp_zones qoz
        join geo_tract gt on qoz.tract_geoid = gt.geoid
)

select avg(median_income) as median_income, 
        avg(ratio_state_median_income) as ratio_state_median_income,
qt.tract_geoid as qualified_geoid,
state,
county_name,
designation


from geo_tract gt join qual_tracts qt on ST_Intersects(gt.geom, qt.geom)
join tract_data td on gt.geoid = td.geoid2
group by qt.tract_geoid,
state,
county_name,
designation;