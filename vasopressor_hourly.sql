
DROP TABLE IF EXISTS public.vasopressor_hourly CASCADE;
CREATE TABLE public.vasopressor_hourly as

with interval_1 as
(
select stay_id 
	, array(select * 
		from generate_series (
			0,round(cast(
				DATE_PART('day', outtime_hr::timestamp - intime_hr::timestamp) * 24 *60 + 
       			DATE_PART('hour', outtime_hr::timestamp - intime_hr::timestamp) * 60 +
       			DATE_PART('minute', outtime_hr::timestamp - intime_hr::timestamp) as numeric))
  				)) as interval_1 
from mimiciv_derived.icustay_times
) 
, interval_2 as 
(
select stay_id
	, start
	, (start) + 1 as end
from interval_1 m
cross join unnest(interval_1) as start
)
, interval as 
(
select * 
from interval_2
 where start < (7 * 24 * 60) 
)
, noradrenalin as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as noradrenalin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as noradrenalin_end_offset
	, max(rate) as noradrenalin 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 221906 and rate < 5.5
-- limit norad to 
group by stay_id, starttime 
)
, phenylephrine as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as phenylephrine_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as phenylephrine_end_offset
	, max(rate) as phenylephrine 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 221749
group by stay_id, starttime 
)
, vasopressin as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as vasopressin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as vasopressin_end_offset
	, max(rate) as vasopressin 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 222315
group by stay_id, starttime 
)
, adrenalin as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as adrenalin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as adrenalin_end_offset
	, max(rate) as adrenalin 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 221289
group by stay_id, starttime 
)
, dopamine as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as dopamine_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as dopamine_end_offset
	, max(rate) as dopamine 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 221662
group by stay_id, starttime 
)
, dobutamin as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as dobutamin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as dobutamin_end_offset
	, max(rate) as dobutamin 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 221653
group by stay_id, starttime 
)
, milrinone as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as milrinone_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as milrinone_end_offset
	, max(rate) as milrinone 
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
	where itemid = 221986
group by stay_id, starttime 
)
, angiotensin as 
( 
select stay_id 
	, starttime
	, min(DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - intime_hr::timestamp)) as angiotensin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - intime_hr::timestamp)) as angiotensin_end_offset
	, max(rate) as angiotensin
from mimiciv_icu.inputevents i  
inner join mimiciv_derived.icustay_times it  
	using (stay_id)
	where itemid in (229709229709, 229764)
group by stay_id, starttime 
)
, combine_1 as 
( 
select i.stay_id 
	, i.start, i.end
	, cast (coalesce (noradrenalin, 0) as numeric) as noradrenalin 
	, cast (coalesce (phenylephrine, 0) as numeric) as phenylephrine
	, cast (coalesce (vasopressin, 0) as numeric)  as vasopressin
	, cast (coalesce (adrenalin, 0) as numeric) as adrenalin
	, cast (coalesce (dopamine, 0) as numeric) as dopamine
	, cast (coalesce (dobutamin, 0) as numeric)  as dobutamin
	, cast (coalesce (milrinone, 0) as numeric)  as milrinone
	, cast (coalesce (angiotensin, 0) as numeric)  as angiotensin
	, cast ((coalesce (noradrenalin, 0) + coalesce (phenylephrine, 0)/10  +  coalesce (vasopressin, 0)/24  
		+ coalesce (adrenalin, 0) + coalesce (dopamine, 0)/100 + coalesce (angiotensin, 0)/100) as numeric) as vasopressor_rate 
from interval i
left join (
	select stay_id
		, start
		, max(noradrenalin) as noradrenalin 
	from interval 
	left join noradrenalin 
		using (stay_id)
			where start >= noradrenalin_begin_offset 
			and start < noradrenalin_end_offset
		group by stay_id, start 
	) n 
	on i.stay_id = n.stay_id and i.start = n.start 
left join (
	select stay_id
		, start
		, max(phenylephrine) as phenylephrine
	from interval 
	left join phenylephrine
		using (stay_id)
			where start >= phenylephrine_begin_offset 
			and start < phenylephrine_end_offset
		group by stay_id, start 
	) p
	on i.stay_id = p.stay_id and i.start = p.start 
left join (
	select stay_id
		, start
		, max(vasopressin) as vasopressin
	from interval 
	left join vasopressin
		using (stay_id)
			where start >= vasopressin_begin_offset 
			and start < vasopressin_end_offset
		group by stay_id, start 
	) v
	on i.stay_id = v.stay_id and i.start = v.start 
left join (
	select stay_id
		, start
		, max(adrenalin) as adrenalin
	from interval 
	left join adrenalin
		using (stay_id)
			where start >= adrenalin_begin_offset 
			and start < adrenalin_end_offset
		group by stay_id, start 
	) a
	on i.stay_id = a.stay_id and i.start = a.start 
left join (
	select stay_id
		, start
		, max(dopamine) as dopamine
	from interval 
	left join dopamine
		using (stay_id)
			where start >= dopamine_begin_offset 
			and start < dopamine_end_offset
		group by stay_id, start 
	) dopa
	on i.stay_id = dopa.stay_id and i.start = dopa.start 
left join (
	select stay_id
		, start
		, max(dobutamin) as dobutamin
	from interval 
	left join dobutamin
		using (stay_id)
			where start >= dobutamin_begin_offset 
			and start < dobutamin_end_offset
		group by stay_id, start 
	) d
	on i.stay_id = d.stay_id and i.start = d.start 
left join (
	select stay_id
		, start
		, max(milrinone) as milrinone
	from interval 
	left join milrinone
		using (stay_id)
			where start >= milrinone_begin_offset 
			and start < milrinone_end_offset
		group by stay_id, start 
	) m 
	on i.stay_id = m.stay_id and i.start = m.start 
left join (
	select stay_id
		, start
		, max(angiotensin) as angiotensin
	from interval 
	left join angiotensin
		using (stay_id)
			where start >= angiotensin_begin_offset 
			and start < angiotensin_end_offset
		group by stay_id, start 
	) angio
	on i.stay_id = angio.stay_id and i.start = angio.start 
order by stay_id, start 
)
,interval_3 as
(
select stay_id 
	, array(select * 
		from generate_series (
			0,round(cast(
				DATE_PART('day', outtime_hr::timestamp - intime_hr::timestamp) * 24 *60 + 
       			DATE_PART('hour', outtime_hr::timestamp - intime_hr::timestamp) * 60 +
       			DATE_PART('minute', outtime_hr::timestamp - intime_hr::timestamp) as numeric) / 60)
       			)) as interval_2               
from mimiciv_derived.icustay_times it 
) 
, interval_4 as 
(
select stay_id
	, start as hr 
	, start * 60 as minute_start 
	, (start * 60 ) + 60 as minute_end 
from interval_3 m
cross join unnest(interval_2) as start
)
, combine_2 as 
( 
select stay_id 
	, hr 
	, avg (noradrenalin) as noradrenalin 
	, avg (phenylephrine) as phenylephrine  
	, avg (vasopressin) as vasopressin 
	, avg (adrenalin) as adrenalin 
	, avg (dopamine) as dopamine 
	, avg (dobutamin) as dobutamin  
	, avg (milrinone) as milrinone 
	, avg (angiotensin) as angiotensin 
	, avg (vasopressor_rate) as vasopressor_rate
from interval_4 i
left join combine_1 c 
	using (stay_id)
	where c.start >= minute_start   
		and c.start < minute_end 
group by stay_id, hr 
)
select * 
from combine_2;




























