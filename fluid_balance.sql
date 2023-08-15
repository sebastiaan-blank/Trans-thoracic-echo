
DROP TABLE IF EXISTS public.fluid_balance CASCADE;
CREATE TABLE public.fluid_balance as


with base as 
(
select stay_id 
	, intime_hr as icu_intime 
	, outtime_hr as icu_outtime 
	, DATE_PART('day', outtime_hr::timestamp - intime_hr::timestamp) * 24 *60 + 
       DATE_PART('hour', outtime_hr::timestamp - intime_hr::timestamp) * 60 +
       DATE_PART('minute', outtime_hr::timestamp - intime_hr::timestamp) as los_min
--from mimiciv_derived.icustay_detail id
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
	, min(infusion_start_offset) as intake_start_offset, min(infusion_end_offset) as intake_end_offset
	, min(starttime) as starttime, min(endtime) as endtime
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
order by charttime 
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
,226561	
,226621	
,227701	
,226571	
,226572	
,226580	
,226559	
,226573	
,227489	
,227488
,226604	
,226605	
,226584	
,226599	
,226600	
,226601	
,226602	
,226574	
,226598	
,226597	
,226565	
,226590	
,226591	
,226610	
,226558	
,226592	
,226575	
,226576	
,226582	
,226623	
,226624	
,226612	
,226619	
,226620	
,226583	
,226622	
,226564	
,226593	
,226595	
,226557	
,226579	
,226567	
,226617	
,226618	
,226563	
,226625	
,227511	
,226603	
,226560	
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
, dialysis_1 as 
( 
select stay_id 
	, charttime 
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
	, charttime 
	, min(dialysis_offset) as dialysis_offset
	, sum(dialysis_output) as dialysis_amount
from dialysis_1 
group by stay_id, charttime 
) 
, d_1 as 
( 
select b.stay_id 
		, round (d1_cristalloid_bolus) as d1_cristalloid_bolus, round(d1_colloid_bolus) as d1_colloid_bolus 
		, round(coalesce (d1_cristalloid_bolus, 0) + coalesce (d1_colloid_bolus,0)) as d1_bolus 
		, round (d1_blood) as d1_blood, round(d1_intake) as d1_intake  
		, round (d1_urine) as d1_urine, round(d1_dialysis) as d1_dialysis    
		, round(coalesce (d1_output,0) + coalesce (d1_dialysis,0)) as d1_output 
		, round(coalesce (d1_intake,0) - (coalesce (d1_output,0) + coalesce (d1_dialysis,0))) as d1_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d1_cristalloid_bolus 
		, sum (colloid_bolus) as d1_colloid_bolus 
	from bolus 
		where bolus_offset >= 0 and bolus_offset < 24 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d1_blood 
	from blood 
	    where blood_start_offset >= 0 and blood_start_offset < 24 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d1_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 0 and intake_end_offset <= 24 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 0 and  intake_end_offset > 24 * 60 then ((1440 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 0 and intake_end_offset <= 24 * 60 then (intake_end_offset/60) * intake_rate
						when intake_start_offset < 0 and intake_end_offset > 24 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 0 and intake_start_offset < 24 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d1_urine
	from urine
	    where urine_offset > 0 and urine_offset <= 24 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d1_output
	from output
	    where output_offset > 0 and output_offset <= 24 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d1_dialysis 
	from dialysis 
	    where dialysis_offset > 0 and dialysis_offset <= 24 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, d_2 as 
( 
select b.stay_id 
		, round (d2_cristalloid_bolus) as d2_cristalloid_bolus, round(d2_colloid_bolus) as d2_colloid_bolus 
		, round(coalesce (d2_cristalloid_bolus, 0) + coalesce (d2_colloid_bolus,0)) as d2_bolus 
		, round (d2_blood) as d2_blood, round(d2_intake) as d2_intake  
		, round (d2_urine) as d2_urine, round(d2_dialysis) as d2_dialysis   
		, round(coalesce (d2_output,0) + coalesce (d2_dialysis,0)) as d2_output 
		, round(coalesce (d2_intake,0) - (coalesce (d2_output,0) + coalesce (d2_dialysis,0))) as d2_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d2_cristalloid_bolus 
		, sum (colloid_bolus) as d2_colloid_bolus 
	from bolus 
		where bolus_offset >= 24 * 60 and bolus_offset < 48 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d2_blood 
	from blood 
	    where blood_start_offset >= 24 * 60 and blood_start_offset < 48 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d2_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 24 * 60 and intake_end_offset <= 48 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 24 * 60 and  intake_end_offset > 48 * 60 then ((48 * 60 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 24 * 60 and intake_end_offset <= 48 * 60 then ((intake_end_offset - 24 * 60)/60) * intake_rate
						when intake_start_offset < 24 * 60 and intake_end_offset > 48 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 24 * 60 and intake_start_offset < 48 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d2_urine
	from urine
	    where urine_offset > 24 * 60 and urine_offset <= 48 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d2_output
	from output
	    where output_offset > 24 * 60 and output_offset <= 48 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d2_dialysis 
	from dialysis 
	    where dialysis_offset > 24 * 60 and dialysis_offset <= 48 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, d_3 as 
( 
select b.stay_id 
		, round (d3_cristalloid_bolus) as d3_cristalloid_bolus, round(d3_colloid_bolus) as d3_colloid_bolus 
		, round(coalesce (d3_cristalloid_bolus, 0) + coalesce (d3_colloid_bolus,0)) as d3_bolus 
		, round (d3_blood) as d3_blood, round(d3_intake) as d3_intake  
		, round (d3_urine) as d3_urine, round(d3_dialysis) as d3_dialysis   
		, round(coalesce (d3_output,0) + coalesce (d3_dialysis,0)) as d3_output 
		, round(coalesce (d3_intake,0) - (coalesce (d3_output,0) + coalesce (d3_dialysis,0))) as d3_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d3_cristalloid_bolus 
		, sum (colloid_bolus) as d3_colloid_bolus 
	from bolus 
		where bolus_offset >= 48 * 60 and bolus_offset < 72 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d3_blood 
	from blood 
	    where blood_start_offset >= 48 * 60 and blood_start_offset < 72 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d3_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 48 * 60 and intake_end_offset <= 72 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 48 * 60 and  intake_end_offset > 72 * 60 then ((72 * 60 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 48 * 60 and intake_end_offset <= 72 * 60 then ((intake_end_offset - 48 * 60)/60) * intake_rate
						when intake_start_offset < 48 * 60 and intake_end_offset > 72 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 48 * 60 and intake_start_offset < 72 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d3_urine
	from urine
	    where urine_offset > 48 * 60 and urine_offset <= 72 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d3_output
	from output
	    where output_offset > 48 * 60 and output_offset <= 72 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d3_dialysis 
	from dialysis 
	    where dialysis_offset > 48 * 60 and dialysis_offset <= 72 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, d_4 as 
( 
select b.stay_id 
		, round (d4_cristalloid_bolus) as d4_cristalloid_bolus, round(d4_colloid_bolus) as d4_colloid_bolus 
		, round(coalesce (d4_cristalloid_bolus, 0) + coalesce (d4_colloid_bolus,0)) as d4_bolus 
		, round (d4_blood) as d4_blood, round(d4_intake) as d4_intake  
		, round (d4_urine) as d4_urine, round(d4_dialysis) as d4_dialysis   
		, round(coalesce (d4_output,0) + coalesce (d4_dialysis,0)) as d4_output 
		, round(coalesce (d4_intake,0) - (coalesce (d4_output,0) + coalesce (d4_dialysis,0))) as d4_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d4_cristalloid_bolus 
		, sum (colloid_bolus) as d4_colloid_bolus 
	from bolus 
		where bolus_offset >= 72 * 60 and bolus_offset < 96 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d4_blood 
	from blood 
	    where blood_start_offset >= 72 * 60 and blood_start_offset < 96 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d4_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 72 * 60 and intake_end_offset <= 96 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 72 * 60 and  intake_end_offset > 96 * 60 then ((96 * 60 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 72 * 60 and intake_end_offset <= 96 * 60 then ((intake_end_offset - 72 * 60)/60) * intake_rate
						when intake_start_offset < 72 * 60 and intake_end_offset > 96 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 72 * 60 and intake_start_offset < 96 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d4_urine
	from urine
	    where urine_offset > 72 * 60 and urine_offset <= 96 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d4_output
	from output
	    where output_offset > 72 * 60 and output_offset <= 96 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d4_dialysis 
	from dialysis 
	    where dialysis_offset > 72 * 60 and dialysis_offset <= 96 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, d_5 as 
( 
select b.stay_id 
		, round (d5_cristalloid_bolus) as d5_cristalloid_bolus, round(d5_colloid_bolus) as d5_colloid_bolus 
		, round(coalesce (d5_cristalloid_bolus, 0) + coalesce (d5_colloid_bolus,0)) as d5_bolus 
		, round (d5_blood) as d5_blood, round(d5_intake) as d5_intake  
		, round (d5_urine) as d5_urine, round(d5_dialysis) as d5_dialysis   
		, round(coalesce (d5_output,0) + coalesce (d5_dialysis,0)) as d5_output 
		, round(coalesce (d5_intake,0) - (coalesce (d5_output,0) + coalesce (d5_dialysis,0))) as d5_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d5_cristalloid_bolus 
		, sum (colloid_bolus) as d5_colloid_bolus 
	from bolus 
		where bolus_offset >= 96 * 60 and bolus_offset < 120 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d5_blood 
	from blood 
	    where blood_start_offset >= 96 * 60 and blood_start_offset < 120 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d5_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 96 * 60 and intake_end_offset <= 120 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 96 * 60 and  intake_end_offset > 120 * 60 then ((120 * 60 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 96 * 60 and intake_end_offset <= 120 * 60 then ((intake_end_offset - 96 * 60)/60) * intake_rate
						when intake_start_offset < 96 * 60 and intake_end_offset > 120 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 96 * 60 and intake_start_offset < 120 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d5_urine
	from urine
	    where urine_offset > 96 * 60 and urine_offset <= 120 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d5_output
	from output
	    where output_offset > 96 * 60 and output_offset <= 120 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d5_dialysis 
	from dialysis 
	    where dialysis_offset > 96 * 60 and dialysis_offset <= 120 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, d_6 as 
( 
select b.stay_id 
		, round (d6_cristalloid_bolus) as d6_cristalloid_bolus, round(d6_colloid_bolus) as d6_colloid_bolus 
		, round(coalesce (d6_cristalloid_bolus, 0) + coalesce (d6_colloid_bolus,0)) as d6_bolus 
		, round (d6_blood) as d6_blood, round(d6_intake) as d6_intake  
		, round (d6_urine) as d6_urine, round(d6_dialysis) as d6_dialysis   
		, round(coalesce (d6_output,0) + coalesce (d6_dialysis,0)) as d6_output 
		, round(coalesce (d6_intake,0) - (coalesce (d6_output,0) + coalesce (d6_dialysis,0))) as d6_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d6_cristalloid_bolus 
		, sum (colloid_bolus) as d6_colloid_bolus 
	from bolus 
		where bolus_offset >= 120 * 60 and bolus_offset < 144 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d6_blood 
	from blood 
	    where blood_start_offset >= 120 * 60 and blood_start_offset < 144 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d6_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 120 * 60 and intake_end_offset <= 144 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 120 * 60 and  intake_end_offset > 144 * 60 then ((144 * 60 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 120 * 60 and intake_end_offset <= 144 * 60 then ((intake_end_offset - 120 * 60)/60) * intake_rate
						when intake_start_offset < 120 * 60 and intake_end_offset > 144 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 120 * 60 and intake_start_offset < 144 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d6_urine
	from urine
	    where urine_offset > 120 * 60 and urine_offset <= 144 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d6_output
	from output
	    where output_offset > 120 * 60 and output_offset <= 144 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d6_dialysis 
	from dialysis 
	    where dialysis_offset > 120 * 60 and dialysis_offset <= 144 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, d_7 as 
( 
select b.stay_id 
		, round (d7_cristalloid_bolus) as d7_cristalloid_bolus, round(d7_colloid_bolus) as d7_colloid_bolus 
		, round(coalesce (d7_cristalloid_bolus, 0) + coalesce (d7_colloid_bolus,0)) as d7_bolus 
		, round (d7_blood) as d7_blood, round(d7_intake) as d7_intake  
		, round (d7_urine) as d7_urine, round(d7_dialysis) as d7_dialysis   
		, round(coalesce (d7_output,0) + coalesce (d7_dialysis,0)) as d7_output 
		, round(coalesce (d7_intake,0) - (coalesce (d7_output,0) + coalesce (d7_dialysis,0))) as d7_fluid_balance 
from base b
left join ( 
	select stay_id 
		, sum (cristalloid_bolus) as d7_cristalloid_bolus 
		, sum (colloid_bolus) as d7_colloid_bolus 
	from bolus 
		where bolus_offset >= 144 * 60 and bolus_offset < 168 * 60 
	group by stay_id 
		) bolus
		on b.stay_id = bolus.stay_id 
left join (
	select stay_id 	
		, sum (blood_amount) as d7_blood 
	from blood 
	    where blood_start_offset >= 144 * 60 and blood_start_offset < 168 * 60 
	group by stay_id 
		) blood
		on b.stay_id = blood.stay_id
left join (
	select stay_id 	
		, sum (intake_amount) as d7_intake 
	from (
		select stay_id 
			,case when intake_rate is not null then 
					case when intake_start_offset >= 144 * 60 and intake_end_offset <= 168 * 60 then ((intake_end_offset - intake_start_offset)/60) * intake_rate
						when intake_start_offset >= 144 * 60 and  intake_end_offset > 168 * 60 then ((168 * 60 -intake_start_offset) /60) * intake_rate
						when intake_start_offset < 144 * 60 and intake_end_offset <= 168 * 60 then ((intake_end_offset - 144 * 60)/60) * intake_rate
						when intake_start_offset < 144 * 60 and intake_end_offset > 168 * 60 then 24 * intake_rate
						else null end 
			 	else intake_amount end as intake_amount 
		from intake
	    	where intake_end_offset > 144 * 60 and intake_start_offset < 168 * 60
	    	) a 
	 group by stay_id 
		) intake
		on b.stay_id = intake.stay_id
left join (
	select stay_id 	
		, sum (urine_amount) as d7_urine
	from urine
	    where urine_offset > 144 * 60 and urine_offset <= 168 * 60 
	group by stay_id 
		) urine
		on b.stay_id = urine.stay_id
left join (
	select stay_id 	
		, sum (output_amount) as d7_output
	from output
	    where output_offset > 144 * 60 and output_offset <= 168 * 60 
	group by stay_id 
		) output
		on b.stay_id = output.stay_id
left join (
	select stay_id 	
		, sum (dialysis_amount) as d7_dialysis 
	from dialysis 
	    where dialysis_offset > 144 * 60 and dialysis_offset <= 168 * 60 
	group by stay_id 
		) dialysis 
		on b.stay_id = dialysis.stay_id
)
, combine as 
(
select b.stay_id 
	, d1_cristalloid_bolus, d1_colloid_bolus, d1_bolus, d1_blood, d1_intake, d1_urine, d1_dialysis, d1_output 
	, d1_fluid_balance 
	, d2_cristalloid_bolus, d2_colloid_bolus, d2_bolus, d2_blood, d2_intake, d2_urine, d2_dialysis, d2_output 
	, case when los_min > 24 * 60 then d2_fluid_balance else null end as d2_fluid_balance
	, case when los_min > 24 * 60 then (d1_fluid_balance + d2_fluid_balance) else null end as d2_cumm_fluid_balance
	, d3_cristalloid_bolus, d3_colloid_bolus, d3_bolus, d3_blood, d3_intake, d3_urine, d3_dialysis, d3_output 
	, case when los_min > 48 * 60 then d3_fluid_balance else null end as d3_fluid_balance
	, case when los_min > 48 * 60 then (d1_fluid_balance + d2_fluid_balance + d3_fluid_balance) else null end as d3_cumm_fluid_balance
	, d4_cristalloid_bolus, d4_colloid_bolus, d4_bolus, d4_blood, d4_intake, d4_urine, d4_dialysis, d4_output 
	, case when los_min > 72 * 60 then d4_fluid_balance else null end as d4_fluid_balance
	, case when los_min > 72 * 60 then (d1_fluid_balance + d2_fluid_balance + d3_fluid_balance + d4_fluid_balance) 
		else null end as d4_cumm_fluid_balance
	, d5_cristalloid_bolus, d5_colloid_bolus, d5_bolus, d5_blood, d5_intake, d5_urine, d5_dialysis, d5_output 
	, case when los_min > 96 * 60 then d5_fluid_balance else null end as d5_fluid_balance
	, case when los_min > 96 * 60 then (d1_fluid_balance + d2_fluid_balance + d3_fluid_balance + d4_fluid_balance + d5_fluid_balance) 
		else null end as d5_cumm_fluid_balance
	, d6_cristalloid_bolus, d6_colloid_bolus, d6_bolus, d6_blood, d6_intake, d6_urine, d6_dialysis, d6_output 
	, case when los_min > 120 * 60 then d6_fluid_balance else null end as d6_fluid_balance
	, case when los_min > 120 * 60 then (d1_fluid_balance + d2_fluid_balance + d3_fluid_balance + d4_fluid_balance + d5_fluid_balance + d6_fluid_balance) 
		else null end as d6_cumm_fluid_balance
	, d7_cristalloid_bolus, d7_colloid_bolus, d7_bolus, d7_blood, d7_intake, d7_urine, d7_dialysis, d7_output 
	, case when los_min > 72 * 60 then d7_fluid_balance else null end as d7_fluid_balance
	, case when los_min > 72 * 60 then (d1_fluid_balance + d2_fluid_balance + d3_fluid_balance + d4_fluid_balance 
				+ d5_fluid_balance + d6_fluid_balance + d7_fluid_balance) else null end as d7_cumm_fluid_balance
	, (d1_fluid_balance + d2_fluid_balance + d3_fluid_balance + d4_fluid_balance + d5_fluid_balance 
		+ d6_fluid_balance + d7_fluid_balance) as overall_7d_fluid
from base b
left join d_1 
	on b.stay_id = d_1.stay_id 
left join d_2 
	on b.stay_id = d_2.stay_id
left join d_3 
	on b.stay_id = d_3.stay_id
left join d_4 
	on b.stay_id = d_4.stay_id
left join d_5 
	on b.stay_id = d_5.stay_id
left join d_6 
	on b.stay_id = d_6.stay_id
left join d_7 
	on b.stay_id = d_7.stay_id
order by stay_id 
)
select * 
from combine 




      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
