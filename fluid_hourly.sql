 
DROP TABLE IF EXISTS public.fluid_hourly CASCADE;
CREATE TABLE public.fluid_hourly as


with base as 
(
select stay_id 
	, intime_hr as icu_intime 
	, outtime_hr as icu_outtime 
	, DATE_PART('day', outtime_hr::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', outtime_hr::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', outtime_hr::timestamp - intime_hr::timestamp) as los_min
from mimiciv_derived.icustay_times it 
)
, cristalloid as 
(
select stay_id 
	, DATE_PART('day', i.starttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.starttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.starttime ::timestamp - icu_intime::timestamp) as infusion_start_offset
    , DATE_PART('day', i.endtime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.endtime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.endtime ::timestamp - icu_intime::timestamp) as infusion_end_offset
	, case when amountuom in ('ml','mL','cm3') then cast (amount as numeric)
			when amountuom in ('L') then cast ((1000* amount) as numeric)
			else null end as amount 
from mimiciv_icu.inputevents i
inner join base
	using (stay_id) 
	where itemid in (225828, 225827, 225158, 225825) 
	and starttime > icu_intime 
	and case when amountuom in ('L') then amount >= 0.25 else amount >= 250 end
	and (ordercategorydescription= 'Bolus' or 
	(rate > 500 and rateuom = 'mL/hour') or 
	( rate > 8.3 and rateuom = 'mL/min' ))
)
, albumin as
(
select	stay_id 
	, DATE_PART('day', i.starttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.starttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.starttime ::timestamp - icu_intime::timestamp) as infusion_start_offset
    , DATE_PART('day', i.endtime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.endtime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.endtime ::timestamp - icu_intime::timestamp) as infusion_end_offset
	, cast (amount as numeric) as amount 
from mimiciv_icu.inputevents i
inner join base b
	using (stay_id) 
	where itemid in (220864, 225795, 225174) ---- 220864 alb 5%, 225795 Dextran 40 225174 hetastarch
	and amount > 100
	and starttime > icu_intime 
	and (rateuom = 'mL/min' or rate > 250)  
)
, bolus as 
(
select stay_id 
	, infusion_start_offset as bolus_offset
	, sum(c.amount) as cristalloid_bolus
	, sum(a.amount) as colloid_bolus 
from cristalloid c 
full outer join albumin a
	using (stay_id, infusion_start_offset) 
	group by stay_id, infusion_start_offset
)  
, blood_1 as 
(
select stay_id
	, orderid 
	, DATE_PART('day', i.starttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.starttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.starttime ::timestamp - icu_intime::timestamp) as infusion_start_offset
    , DATE_PART('day', i.endtime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.endtime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.endtime ::timestamp - icu_intime::timestamp) as infusion_end_offset
	, case when rateuom = 'mL/min' then rate * 60 else rate end as rate 
	, cast (amount as numeric) as amount
from mimiciv_icu.inputevents i
inner join base
	using (stay_id)
	where itemid in (225171, 220970, 225168, 225170) 
	and i.starttime > icu_intime 
order by stay_id, starttime 
)
, blood as 
(
select stay_id, orderid 
	, min(infusion_start_offset) as blood_start_offset, min(infusion_end_offset) as blood_end_offset
	, sum(amount) as blood_amount, sum(rate) as blood_rate
from blood_1
group by stay_id, orderid 
)
, intake_1 as 
(
select stay_id 
	, orderid
	, DATE_PART('day', starttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', starttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', starttime ::timestamp - icu_intime::timestamp) as infusion_start_offset
    , DATE_PART('day', endtime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', endtime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', endtime ::timestamp - icu_intime::timestamp) as infusion_end_offset
    , starttime, endtime 
	, case when amountuom in ('ml','mL','cm3') then cast (amount as numeric)
			when amountuom in ('uL') then cast ((amount/1000) as numeric)
			when amountuom in ('L') then cast ((1000* amount) as numeric)
			else null end as amount
	, case when rateuom = 'mL/hour' then cast (rate as numeric)
			when rateuom = 'mL/min' then cast (rate * 60 as numeric)
			when rateuom = 'mL/kg/hour' then cast (rate * patientweight as numeric)
				else null end as rate  
from mimiciv_icu.inputevents i
inner join base 
	using (stay_id)
	where  itemid in (
220862	
,220864	
,225173	
,225171	
,225795	
,220970	
,225174	
,227530	
,225168	
,225170		
,225823	
,225941	
,225827	
,225825	
,220950	
,228140	
,228141	
,228142	
,220949	
,220952	
,225797	
,225799	
,226453	
,225828	
,225830	
,225159	
,225158	
,228341	
,225161	
,226089	
,226452	
,227533	
,225943	
,225944		
,227977	
,227976	
,227978	
,227979	
,228355	
,226875	
,225937	
,226877	
,227698	
,227699	
,227696	
,227695	
,228356	
,229013	
,229295	
,228359	
,226023	
,226020	
,226022	
,221207	
,226027	
,226024	
,226026	
,225928	
,228131	
,228132	
,228133	
,228134	
,228135	
,229010	
,229011	
,228348	
,228351	
,227973	
,227974	
,227975	
,226019	
,226016	
,226017	
,227518	
,225931	
,226882	
,226881	
,226880	
,226031	
,226028	
,226030	
,221036	
,229297	
,226039	
,226036	
,226038	
,225930	
,228383	
,225929	
,229009	
,229014	
,229014	
,228360	
,228361	
,228363	
,226047	
,226044	
,226045	
,226046	
,225935	
,226051	
,226048	
,226049	
,226050	
,225936	
,228364	
,228367	
,229012	
,229296	
,226059	
,226058	
,225934		
,227090	
,225801	
,225920	
,225917	
,225916	
,229583	
,226000	
,229574	
,225995	
,225996	
,225994	
,225991	
,226002		
)
order by stay_id, orderid, starttime  
)
, intake as 
(
select stay_id, orderid  
	, min(infusion_start_offset) as intake_start_offset
	, min(infusion_end_offset) as intake_end_offset
--	, min(starttime) as starttime
--	, min(endtime) as endtime
	, sum(amount) as intake_amount, sum(rate) as intake_rate 
from intake_1 
group by stay_id, orderid  
)
, urine as 
(
select stay_id 
	,DATE_PART('day', charttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', charttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', charttime ::timestamp - icu_intime::timestamp) as urine_offset
    , charttime as urine_time 
    , cast(urineoutput as numeric) as urine_amount  
from mimiciv_derived.urine_output uo
inner join base 
	using (stay_id)
order by stay_id, charttime 
)
, urine_rate as 
(
select stay_id 
	, urine_start_offset, urine_end_offset 
	, urine_amount 
	, urine_amount/((urine_end_offset - urine_start_offset)/60) as urine_rate 
from (
	select stay_id
			, urine_offset as urine_end_offset 
			, case when lag (urine_offset) over (partition by stay_id order by urine_offset) is null then 0 
				else lag (urine_offset) over (partition by stay_id order by urine_offset) end as urine_start_offset
			, urine_amount 
	from urine 
	) r
)
, output_1 as 
(
select stay_id 
	, charttime  
	,DATE_PART('day', charttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', charttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', charttime ::timestamp - icu_intime::timestamp) as output_offset
	, case when itemid = 227488 and cast(value as numeric) > 0 then -1 * cast (value as numeric) 
				else cast (value as numeric) end as output 
from mimiciv_icu.outputevents 
inner join base 
	using (stay_id)
      where 
     itemid in (
226616	
,226569	
,226570	
,226632	
,226608	
,226609	
,226606	
,226607	
,226588	
,226589	
,229413	
,229414	
-- ,226561	
,226621	
,227701	
,226571	
,226572	
,226580	
--,226559	
,226573	
--,227489	
--,227488	
,226604	
,226605	
--,226584	
,226599	
,226600	
,226601	
,226602	
,226574	
,226598	
,226597
--,226565	
,226590	
,226591	
,226610	
--,226558	
,226592	
,226575	
,226576	
--,226626	
--,226627	
,226582	
,226623	
,226624	
,226612	
,226619	
,226620		
,226583	
,226622	
--,226564	
,226593	
,226595	
--,226557	
,226579	
--,226567	
,226617	
,226618	-
--,226563	
,226625	
--,227510	
,227511
,226603	
--,226560	
,226613	
,226614	
)
)
, output as 
(
select stay_id 
	, charttime 
	, min (output_offset) as output_offset 
	, sum(output) as output_amount 
from output_1 
group by stay_id, charttime 
) 
----------------- now deal with dialysis 
, dialysis_1 as 
( 
select stay_id 
	, charttime 
	, itemid
	,DATE_PART('day', charttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', charttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', charttime ::timestamp - icu_intime::timestamp) as dialysis_offset
	, case when itemid = 225806 and cast(value as numeric) > 0 then -1 * cast (value as numeric) 
				else cast (value as numeric) end as dialysis_output 
from mimiciv_icu.chartevents 
inner join base 
	using (stay_id)
	where itemid in ( 
226499	
,225806	
,225807	
,226457 
)
)
, dialysis as 
(
select stay_id 
	, min(dialysis_offset) as dialysis_offset
	, sum(dialysis_output) as dialysis_amount
from dialysis_1 
group by stay_id, charttime 
)
, interval_1 as
(
select stay_id 
	, array(select * 
		from generate_series (
			0,round(cast(
				DATE_PART('day', icu_outtime::timestamp - icu_intime::timestamp) * 24 *60 + 
       			DATE_PART('hour', icu_outtime::timestamp - icu_intime::timestamp) * 60 +
       			DATE_PART('minute', icu_outtime::timestamp - icu_intime::timestamp) as numeric) / 60)
       			)) as interval             
from base
) 
, interval as 
(
select stay_id
	, start as hr 
	, start * 60 as minute_start 
	, (start * 60 ) + 60 as minute_end 
from interval_1 m
cross join unnest(interval) as start
)
, combine_1 as 
( 
select i.stay_id 
	, i.hr 
	, minute_start, minute_end 
	, coalesce (intake, 0) as hourly_intake 
	, coalesce (output_amount, 0) + coalesce (urine, 0) + coalesce (dialysis, 0) as hourly_output 
	, coalesce (urine, 0) as hourly_urine 
	, coalesce (est_urine, 0) as est_hourly_urine
	, coalesce (dialysis, 0) as hourly_dialysis 
	, coalesce (intake, 0) - (coalesce (output_amount, 0) + coalesce (dialysis, 0) + coalesce (urine, 0)) as hourly_balance 
	, coalesce (intake, 0) - (coalesce (output_amount, 0) + coalesce (dialysis, 0) + coalesce (est_urine, 0)) as est_hourly_balance
	, coalesce (cristalloid_bolus, 0) as hourly_cristalloid_bolus 
	, coalesce (colloid_bolus, 0) as hourly_colloid_bolus 
	, coalesce (blood, 0) as hourly_blood 
from interval i
left join (
	select stay_id 	
		, hr 
		, sum (intake_amount) as intake 
	from (
		select stay_id 
			, hr 
			,case when intake_rate is not null then 
					case when intake_start_offset >= minute_start and intake_end_offset <= minute_end then intake_amount 
						when intake_start_offset >= minute_start and  intake_end_offset > minute_end 
							then ((minute_end - intake_start_offset)/60) * intake_rate
						when intake_start_offset < minute_start and intake_end_offset <= minute_end 
							then ((intake_end_offset - minute_start) /60) * intake_rate
						when intake_start_offset < minute_start and intake_end_offset > minute_end then intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
		inner join interval 
			using (stay_id)
	    	where intake_start_offset < minute_end and intake_end_offset > minute_start  
	    	) a 
	 group by stay_id, hr  
		) intake
		on i.stay_id = intake.stay_id and i.hr = intake.hr 
left join (
	select stay_id 	
		, hr 
		, sum (urine_amount) as urine
	from interval 
	inner join urine
		using (stay_id)
	    where urine_offset > minute_start and urine_offset <= minute_end
	group by stay_id, hr 
		) urine
		on i.stay_id = urine.stay_id and i.hr = urine.hr
left join (
	select stay_id 	
		, hr
		, sum (output_amount) as output_amount
	from interval 
	inner join output
	using (stay_id)
	    where output_offset > minute_start and output_offset <= minute_end 
	group by stay_id, hr 
		) output
		on i.stay_id = output.stay_id and i.hr = output.hr
left join (
	select stay_id 	
		, hr 
		, sum (dialysis_amount) as dialysis 
	from interval 
	inner join dialysis
		using (stay_id) 
	    where dialysis_offset > minute_start and dialysis_offset <= minute_end 
	group by stay_id, hr 
		) dialysis 
		on i.stay_id = dialysis.stay_id and i.hr = dialysis.hr
left join (
	select stay_id 	
		, hr 
		, sum (cristalloid_bolus) as cristalloid_bolus
		, sum (colloid_bolus) as colloid_bolus 
	from interval 
	inner join bolus
		using (stay_id) 
	    where bolus_offset >= minute_start and bolus_offset < minute_end 
	group by stay_id, hr 
		) bolus 
		on i.stay_id = bolus.stay_id and i.hr = bolus.hr
left join (
	select stay_id 	
		, hr 
		, sum (blood_amount) as blood 
	from (
		select stay_id 
			, hr 
			,case when blood_rate is not null then 
					case when blood_start_offset >= minute_start and blood_end_offset <= minute_end then blood_amount 
						when blood_start_offset >= minute_start and  blood_end_offset > minute_end 
							then ((minute_end - blood_start_offset)/60) * blood_rate
						when blood_start_offset < minute_start and blood_end_offset <= minute_end 
							then ((blood_end_offset - minute_start) /60) * blood_rate
						when blood_start_offset < minute_start and blood_end_offset > minute_end then blood_rate
						else null end 
			 	else blood_amount end as blood_amount 
		from blood
		inner join interval 
			using (stay_id)
	    	where blood_start_offset < minute_end and blood_end_offset > minute_start  
	    	) a 
	 group by stay_id, hr  
		) blood
		on i.stay_id = blood.stay_id and i.hr = blood.hr
left join (
	select stay_id 	
		, hr 
		, sum (urine_amount) as est_urine  
	from (
		select stay_id 
			, hr 
			,case when urine_rate is not null then 
					case when urine_start_offset >= minute_start and urine_end_offset <= minute_end then urine_amount 
						when urine_start_offset >= minute_start and  urine_end_offset > minute_end 
							then ((minute_end - urine_start_offset)/60) * urine_rate
						when urine_start_offset < minute_start and urine_end_offset <= minute_end 
							then ((urine_end_offset - minute_start) /60) * urine_rate
						when urine_start_offset < minute_start and urine_end_offset > minute_end then urine_rate
						else null end 
			 	else 0 end as urine_amount 
		from urine_rate
		inner join interval 
			using (stay_id)
	    	where urine_start_offset < minute_end and urine_end_offset > minute_start  
	    	) a 
	 group by stay_id, hr  
		) urine_rate 
		on i.stay_id = urine_rate.stay_id and i.hr = urine_rate.hr
)
, combine as 
( 
select stay_id 
	, hr 
	, round (hourly_intake) as hourly_intake , round (hourly_output) as hourly_output
	, round(hourly_balance) as hourly_balance  
	, round (sum(hourly_balance) over (partition by stay_id order by hr rows unbounded preceding)) as cumm_hourly_balance 
	, round (sum(est_hourly_balance) over (partition by stay_id order by hr rows unbounded preceding)) as est_cumm_hourly_balance
	, round (hourly_urine) as hourly_urine , round(est_hourly_urine) as est_hourly_urine 
	, round (hourly_dialysis) as hourly_dialysis  
	, round (hourly_cristalloid_bolus) as hourly_cristalloid_bolus , round (hourly_colloid_bolus) as hourly_colloid_bolus
	, round (hourly_blood) as hourly_blood 
from combine_1
order by stay_id, hr 
) 
select *
from combine 
	












      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      


