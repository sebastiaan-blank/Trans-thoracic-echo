---- 
---- Create table with details around tte done in ICU 
--- this script will create a table with all tte in a single icu stay per row. 
--- tte_offset is number of minutes from icu admission that the recorded tte begin time is
--- the offset correspons to the icu_stay_times from the derived table - ie those derived from first /last measure HR..
--- the reason for this is that makes easier for analyses with time varying confounders 
--- this is easily changed in the first part of the script   
--- 
DROP TABLE IF EXISTS public.tte CASCADE;
CREATE TABLE public.tte as

with tte_1 as 
(
select stay_id 
	, starttime as tte_start, endtime as tte_end, storetime as tte_store 
	, DATE_PART('day', p.starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
        DATE_PART('hour', p.starttime::timestamp - intime_hr::timestamp) * 60 +
        DATE_PART('minute', p.starttime::timestamp - intime_hr::timestamp) as tte_offset 
	, 1 as TTE 
	, intime_hr as intime, outtime_hr as outtime
	, rank () over(partition by stay_id order by starttime) as ln 
from mimiciv_derived.icustay_times it 
left join mimiciv_icu.procedureevents p 
	using (stay_id)
	where itemid = 225432
order by p.starttime 
)
-------
----- Remove tte if done within 1 hour of a previous tte (ie consider this a single tte that starts at the earliest begin time) 
-----
, tte_2 as 
(
select * 
	, lag (tte_offset) over (partition by stay_id order by ln) as previous_tte_offset
from tte_1
	where tte_offset < (DATE_PART('day', outtime::timestamp - intime::timestamp) * 24 *60 + 
        DATE_PART('hour', outtime::timestamp - intime::timestamp) * 60 +
        DATE_PART('minute', outtime::timestamp - intime::timestamp))
) 
, tte as 
(
select stay_id 
	, tte_offset, tte_start, tte_end, tte_store, tte 
	, rank () over(partition by stay_id order by tte_offset) as rn
from tte_2
	where ln = 1  
	or (tte_offset - previous_tte_offset) > 60  
)
------ 
--- total number tte per icu stay admission 
--- 
, tte_number as 
( 
select stay_id 
	, max (rn) as tte_number 
from tte
group by stay_id
)
---- 
--- pivot the tte 
---- gives a list of tte per ICU stay 
----- 
, pivot as
(
select stay_id
	, t[1] tte_offset1, n[1] tte_start1, g[1] tte_end1, u[1] tte_store1
	, t[2] tte_offset2, n[2] tte_start2, g[2] tte_end2, u[2] tte_store2
	, t[3] tte_offset3, n[3] tte_start3, g[3] tte_end3, u[3] tte_store3
	, t[4] tte_offset4, n[4] tte_start4, g[4] tte_end4, u[4] tte_store4
	, t[5] tte_offset5, n[5] tte_start5, g[5] tte_end5, u[5] tte_store5
	, t[6] tte_offset6, n[6] tte_start6, g[6] tte_end6, u[6] tte_store6
	, t[7] tte_offset7, n[7] tte_start7, g[7] tte_end7, u[7] tte_store7
	, t[8] tte_offset8, n[8] tte_start8, g[8] tte_end8, u[8] tte_store8
	, t[9] tte_offset9, n[9] tte_start9, g[9] tte_end9, u[9] tte_store9
	, t[10] tte_offset10, n[10] tte_start10, g[10] tte_end10, u[10] tte_store10
	, t[11] tte_offset11, n[11] tte_start11, g[11] tte_end11, u[11] tte_store11
	, t[12] tte_offset12, n[12] tte_start12, g[12] tte_end12, u[12] tte_store12
	, t[13] tte_offset13, n[13] tte_start13, g[13] tte_end13, u[13] tte_store13
	, t[14] tte_offset14, n[14] tte_start14, g[14] tte_end14, u[14] tte_store14
	, t[15] tte_offset15, n[15] tte_start15, g[15] tte_end15, u[15] tte_store15
from (
    select stay_id
    , array_agg(tte_offset order by tte_offset) t
    , array_agg(tte_start order by tte_offset) n
    , array_agg(tte_end order by tte_offset) g
    , array_agg(tte_store order by tte_offset) u 
    from tte
    group by stay_id 
    order by stay_id, t
    ) a
)
, icu_tte as 
(
select  p.*
	, tte_number
from pivot p 
inner join tte_number n 
	using (stay_id)
)
select *
from icu_tte 





