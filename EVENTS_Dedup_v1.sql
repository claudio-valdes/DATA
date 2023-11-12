with normalized_base as (
select e.event_id,
       e.event_uuid,
       e.event_name,
       e.event_historic_name,
       NULLIF(e.event_url, '') as event_url,
       e.date_start,
       e.date_end, 
       e.static_slug,
       NULLIF(city, '') as city,
       NULLIF(state, '') as state,
       NULLIF(country, '') as country,
       NULLIF(event_organizer_name, '') as event_organizer_name,
       LOWER(REGEXP_REPLACE(date_start, '[^a-zA-Z0-9]', '')) as date_token,
       LOWER(REGEXP_REPLACE(city, '[^a-zA-Z0-9]', '')) as city_token,
       LOWER(REGEXP_REPLACE(static_slug, '[^a-zA-Z0-9]', '')) as slug_token,
       row_number()over(partition by e.event_uuid order by e.event_id desc) as exact_events, --- found about 90+ events that were nearly exact copies, taking only one 
       case when fa.event_uuid is not null then 'yes' else null end as is_fusion
       
from DAGSTER.PROD.EVENTS e
left join DAGSTER.PROD.CUSTOMER_FUSION_ACTIVATIONS fa on e.event_uuid = fa.event_uuid
-- JACOB REPORT: where date_start between '2024-01-01' and '2024-12-31'
-- TESTING_v1: where e.event_id in (20115,22629,79605,133506,84387,142199,94722,122562,95785,137072,98364,122010,72676,115217,89971,141060,141774,69260,142324,78456,23639,23639,106582,106582,133674,95843,136997,74266,63264,152495,19292,19292,144244,144244,49065,49062,152361,153983,2057,3466,3470,1371,130616,83612,30940,25183,133120,102201,132713,132705,128837,128985,99354,130423,7338,7465,114599,114599,4430,7442,68295,129149,70366,141883,2371,1242,122581,132082,7445,6440,5611,5540,6913,47294,47298,66962,66962,80802,121990,24146,24146,8202,8202,27908,32940,19148,20157,20086,19994,20329,20684,19513,20818,21226,20937,20699,21171,21423,20712,100432,89524,100178,135292,138982,72598,139826,83671,143227,73602,134340,143138,92226,91591,144723,78244,131084,131082,139919,139896,17929,17929,56874,56874,56874,56874,26996,26996,11808,11808,56290,56290,12464,12696,12493,25622,25622,17678,17678,17678,23580,23580,128668,91566,91348,122240,93505,135578,90930,138854,5127,4389,14879,14855,132883,102750,134113,69876,129363,93490,137359,90571,91769,141128,122678,90862,92287,134959,92454,135703,130397,83570,125736,98681,116259,116261,99915,135116,20134,18545,152691,58086,130992,94274,99715,128223,65357,65357,124001,83713,98612,139335,157395,59198,27400,27400,154671,145819,56760,56760,143204,98901,160554,115031,115031,22113,22113,23580,11808,115031,114599,8202,144244,56760,26996,56874,24146,22113,106582,19292,56290,17929,65357,25622,27400,66962,17678,23639)
)
---- LAYER 1: remove exact duplicates 
, base as (
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       event_historic_name, 
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       is_fusion,
       date_token,
       city_token,
       slug_token,
       concat(coalesce(slug_token,''),coalesce(date_token,''),coalesce(city_token,'')) as event_identifier
from normalized_base
where exact_events = 1 
)
---- LAYER 2: remove exact urls (FIX! ONLY REMOVE IF YEAR IS SHARED)
, dups_url_year as (
SELECT event_id, 
       event_uuid,
       static_slug,
       event_name,
       event_historic_name, 
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       date_token,
       city_token,
       slug_token,
       event_identifier,
       is_fusion,
       extract(year from date_start) as event_year,  -- Extract year from date_start
       rank() OVER (PARTITION BY event_url order by
                    coalesce(event_historic_name, '')desc,
                    coalesce(date_start, '')desc,
                    coalesce(date_end, '')desc,
                    coalesce(city, '')desc,
                    coalesce(state, '')desc,
                    coalesce(country, '')desc,
                    coalesce(event_organizer_name, '')desc,
                    coalesce(is_fusion, '')desc
                    ) as rank_url_dup
from base
where event_url is not null
)
, deduped_event_url as (
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       event_historic_name, 
       event_year,
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       date_token,
       city_token,
       slug_token,
       event_identifier,
       is_fusion

from dups_url_year
where event_id in (select event_id from dups_url_year where rank_url_dup = 1) 
and exists (select 1
            FROM dups_url_year d2
            WHERE d2.event_url = dups_url_year.event_url
            AND d2.rank_url_dup = 1)
)
, base_1 as (
select * from deduped_event_url
union 
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       event_historic_name, 
       extract(year from date_start) as event_year,
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       date_token,
       city_token,
       slug_token,
       event_identifier,
       is_fusion
from base
where not event_url in (select event_url from deduped_event_url)
)

---- LAYER 3: remove exact event_tokens that were created by adding slug+date_start+city 

, dups_1 as (
select event_identifier, count(event_identifier) from base_1
group by 1 
having count(event_identifier) > 1
)
, dups_2 as (
select b.* 

from base_1 b
inner join dups_1 d on b.event_identifier = d.event_identifier 
order by b.event_identifier
)
, dup_rank as (
select *, 
       row_number()over(partition by event_identifier order by 
                                   coalesce(event_historic_name,'')desc ,
                                   coalesce(event_url,'') desc,
                                   coalesce(date_start,'')desc ,
                                   coalesce(date_end,'')desc ,
                                   coalesce(city,'') desc,
                                   coalesce(state,'') desc,
                                   coalesce(country,'') desc,
                                   coalesce(event_organizer_name,'') desc, 
                                   coalesce(is_fusion, '') desc) as rank_slug   --- we are ranking according to data completeness 

from dups_2
)
, deduped_event_identifier as (
select * from dup_rank 
where rank_slug = 1 ----- taking the one that has more data.
)
, base_2 as (
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       event_historic_name, 
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       date_token,
       city_token,
       slug_token,
       event_identifier,
       is_fusion

from deduped_event_identifier
union 
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       event_historic_name, 
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       date_token,
       city_token,
       slug_token,
       event_identifier,
       is_fusion
       
from base_1
where not event_identifier in (select event_identifier from deduped_event_identifier)
)
, event_signature as (
select event_id,
       array_agg(event_identifier) as event_identifier_array

from base_2
group by event_id
)
, identifier_similarity_2 as (
select s1.event_id as event_id_1,
       s2.event_id as event_id_2,
       jarowinkler_similarity(s1.event_identifier, s2.event_identifier) as event_jarow_sim
from base_2 s1
cross join base_2 s2
where s1.event_id < s2.event_id
)
--------------- for query optimization, only looking for similarity in events taking place in the same year and month
, same_year_month_similarity as (
select is1.event_id_1, 
       is1.event_id_2, 
       is1.event_jarow_sim as similarity_score
       
from identifier_similarity_2 is1
inner join base_2 b1 on is1.event_id_1 = b1.event_id
inner join base_2 b2 on is1.event_id_2 = b2.event_id
where date_part(year, b1.date_start) = date_part(year, b2.date_start)
  and date_part(month, b1.date_start) = date_part(month, b2.date_start)
 -- and b1.city = b2.city  --- should we add this filter?
)
, array_similarity_score as (
select event_id_1 as event_id, 
       similarity_score, 
       array_agg(event_id_2) as similar_events_array
       
from same_year_month_similarity
group by 1,2
)
, similaritygroups as(
select event_id as group_id,
       event_id as event_in_group,
       similarity_score
       
from array_similarity_score
where similarity_score >= 50 

)
,similaritygroups_2 as (
select sg.group_id,
       s.event_id_2 as event_in_group,
       s.similarity_score
       
from similaritygroups sg
inner join same_year_month_similarity s on sg.event_in_group = s.event_id_1
where s.similarity_score >= 50
)
, similarity_groups_all as (  ---- grouped all events that share the same year and month + have a similarity score of >90

select * from similaritygroups
union all
select * from similaritygroups_2
)
, event_groups as (
select distinct group_id,
       similarity_score,
       event_in_group
       
from similarity_groups_all
order by group_id
) 
, pre_base_3 as (
select b2.event_id, 
       b2.event_uuid,
       b2.static_slug,
       b2.event_name,
       avg(eg.similarity_score)::int as similarity_score,
       eg.group_id,
       b2.event_historic_name, 
       b2.event_url,
       b2.date_start,
       b2.date_end,
       b2.city,
       b2.state,
       b2.country,
       b2.event_organizer_name,
       b2.is_fusion
       
      
from base_2 b2
left join event_groups eg on b2.event_id = eg.event_in_group
group by 1,2,3,4,6,7,8,9,10,11,12,13,14,15
order by group_id
)
, dup_rank_2 as (
select *, 
       row_number()over(partition by group_id order by 
                                   coalesce(event_historic_name,'')desc ,
                                   coalesce(event_url,'') desc,
                                   coalesce(date_start,'')desc ,
                                   coalesce(date_end,'')desc ,
                                   coalesce(city,'') desc,
                                   coalesce(state,'') desc,
                                   coalesce(country,'') desc,
                                   coalesce(event_organizer_name,'') desc,
                                   coalesce(is_fusion,'') desc) as rank_group ----- rank by data completeness 

from pre_base_3
--where similarity_score >= 96  -------- based on sampling: We are keeping only one event from groups that have a similarity score higher or equal to 96 
)
------ LAYER3: (view: removing events that are extremely similar, keeping only the one with the most data, not applying to set atm)

, deduped_base_3 as (
select * from dup_rank_2 
--where rank_group = 1 ----- taking the record with the most complete data from those groups 
)
, base_3 as (
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       group_id,
       similarity_score,
       event_historic_name, 
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       is_fusion
from deduped_base_3 
union
select event_id, 
       event_uuid,
       static_slug,
       event_name,
       group_id,
       similarity_score,
       event_historic_name, 
       event_url,
       date_start,
       date_end,
       city,
       state,
       country,
       event_organizer_name,
       is_fusion
       
from pre_base_3 
where not group_id in (select group_id from deduped_base_3)
)
, final as (
select b3.event_id,
       b3.is_fusion,
       b3.event_uuid,
       b3.static_slug,
       b3.event_name,
       b3.group_id,
       b3.event_historic_name, 
       b3.event_url,
       b3.date_start,
       b3.date_end,
       b3.city,
       b3.state,
       b3.country,
       b3.event_organizer_name,
       b3.similarity_score,
       array_agg(event_in_group) as grouped_event_ids
      

from base_3 b3
left join event_groups eg on b3.group_id = eg.group_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
order by b3.group_id
)
select * from final order by group_id, similarity_score
