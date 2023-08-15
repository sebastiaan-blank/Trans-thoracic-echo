with base_1 as 
(
select i.subject_id , i.hadm_id, i.stay_id 
	, gender, admission_age as age, id.admittime
		, intime_hr as icu_intime	
		, id.icu_outtime
		, case when id.race ilike '%asian%' then 'Asian'
				when id.race ilike '%black%' then 'Black'
				when id.race ilike '%white%' then 'White'
				when id.race ilike '%hispanic%' or id.race ilike '%south american%' then 'Hispanic'
				when id.race ilike '%alaska%' then 'American Native'
				when id.race ilike '%pacific%' then 'Native Hawaiian'
				when id.race ilike '%unkown%' or id.race ilike '%unable%' or id.race ilike '%declined%' then 'Unknown'
				else 'other' end as race				
		, los_icu
		, case when id.hospital_expire_flag = 1 then 1 else 0 end as hosp_mortality
		, case when id.hospital_expire_flag = 1 then deathtime else id.dischtime end as dischtime 
		, dod, DATE_PART('day', dod::timestamp - id.admittime::timestamp) as day_till_dead
		, first_careunit , first_service 
		, DATE_PART('day', icu_intime::timestamp - id.admittime::timestamp) * 24 *60 + 
       		 DATE_PART('hour', icu_intime::timestamp - id.admittime::timestamp) * 60 +
       		 DATE_PART('minute', icu_intime::timestamp - id.admittime::timestamp) as icu_admit_offset
        , case when deathtime is not null and deathtime::timestamp <= icu_outtime::timestamp + interval '4 hours' then 1 else 0 end as icu_mortality   
from mimiciv_icu.icustays i 
inner join mimiciv_derived.icustay_detail id 
	 on i.stay_id = id.stay_id
inner join mimiciv_derived.icustay_times it2 
	on i.stay_id = it2.stay_id 
inner join mimiciv_hosp.admissions a2 
	on i.hadm_id = a2.hadm_id 
left join (
	select distinct on (hadm_id) hadm_id 
		, curr_service as first_service 
	from mimiciv_hosp.services 
	order by hadm_id, transfertime 
	) s 
	on i.hadm_id = s.hadm_id 
)
, cardio as 
(
select stay_id 
	, max (case when icd_code in (
'I4901',  	
'42741',  	
'I472',   	
'4271',   	
'I472',  		
'42789',  		
'42613',  	
'4275',   	
'I469',   	
'I468',   	
'I462',   	
'I97121', 	
'R001',	 	
'I2102',  	
'I2121',  	
'I2101',  	
'I2109',  	
'I2119',  	
'I2129',  	
'I2111',  	
'I213',   	
'4260',   	
'I442'   	
		) and seq_num < 3 then 1 else 0 end) as card_arrest
from base_1 
inner join mimiciv_hosp.diagnoses_icd di2 
	using (hadm_id)
group by stay_id 
)
, base_2 as 
(
select * 
from base_1 
inner join cardio 
	using (stay_id)
	where 
	first_careunit not in ('Coronary Care Unit (CCU)','Cardiac Vascular Intensive Care Unit (CVICU)') and 
	first_service not in ('CSURG','CMED') and 
	icu_admit_offset < 24 * 60 
	and  card_arrest = 0
)
, base_3 as 
( 
select b2.* 
	, antibiotic_time , culture_time, suspected_infection_time
	, sofa_time as sepsis_time, sofa_score as sofa_sepsis
	, respiration as sofa_sepsis_resp, coagulation as sofa_sepsis_coag, liver as sofa_sepsis_liver
	, cardiovascular as sofa_sepsis_cardio, cns as sofa_sepsis_cns, renal as sofa_sepsis_renal
	, (DATE_PART('day', dischtime::timestamp - admittime::timestamp) * 24 *60 + 
        DATE_PART('hour', dischtime::timestamp - admittime::timestamp) * 60 +
        DATE_PART('minute', dischtime::timestamp - admittime ::timestamp)) / (24*60) as los_hospital
	, (DATE_PART('day', sofa_time::timestamp - admittime::timestamp) * 24 *60 + 
        DATE_PART('hour', sofa_time::timestamp - admittime::timestamp) * 60 +
        DATE_PART('minute', sofa_time::timestamp - admittime ::timestamp)) as sepsis_offset_admit
from base_2 b2 
inner join mimiciv_derived.sepsis3
	using (stay_id)
		where (DATE_PART('day', sofa_time::timestamp - admittime::timestamp) * 24 *60 + 
        DATE_PART('hour', sofa_time::timestamp - admittime::timestamp) * 60 +
        DATE_PART('minute', sofa_time::timestamp - admittime ::timestamp)) < 24 * 60 
)
, base_4 as 
( 
select distinct on (subject_id) * 
from base_3
order by subject_id, icu_intime 
)
, base_5 as 
(
select b4.* 
	, case when edregtime is not null then 1 else 0 end as ED_presentation 
	, edregtime, edouttime
	, case when anchor_year_group = '2008 - 2010' then 2008 + (DATE_PART('year', b4.admittime::timestamp) - anchor_year)
			when anchor_year_group = '2011 - 2013' then 2011 + (DATE_PART('year', b4.admittime::timestamp) - anchor_year)
			when anchor_year_group = '2014 - 2016' then 2014 + (DATE_PART('year', b4.admittime::timestamp) - anchor_year)
			when anchor_year_group = '2017 - 2019' then 2017 + (DATE_PART('year', b4.admittime::timestamp) - anchor_year)
			else null end as admit_year 
from base_4 b4
inner join mimiciv_hosp.admissions a 
	on b4.hadm_id = a.hadm_id 
inner join mimiciv_hosp.patients p3 
	on b4.subject_id = p3.subject_id 
)
, base_6 as 
(
select b5.* 
	, case when admit_year < 2011 then '2008 - 2010' 
			when admit_year in (2011, 2012, 2013) then '2011 - 2013'
			when admit_year in (2014, 2015, 2016) then '2014 - 2016'
			when admit_year > 2016 then '2017 - 2019'
	 else null end as admit_period
from base_5 b5 
) 
,renal_1 as 
( 
select hadm_id 
	, max (case when icd_code in ('Z992', 'V4511') then 1
		else 0 end) as hx_dialysis 
from mimiciv_hosp.diagnoses_icd di 
group by hadm_id 
)
, renal_2 as
(
select stay_id 
	, max (aki_stage) as aki_stage 
from mimiciv_derived.kdigo_stages 
group by stay_id 
)  
, renal_3 as 
(
select stay_id 
	, max (case when dialysis_active > 0 then 1 else 0 end) as dialysis 
from mimiciv_derived.rrt 
group by stay_id 
)
, renal as 
( 
select b.stay_id 
	, case when hx_dialysis is null then 0 else hx_dialysis end as hx_dialysis
	, case when hx_dialysis = 1 then 0 
			when hx_dialysis = 0 and dialysis = 1 then 3
			else aki_stage end as kdigo 
from base_6 b 
left join renal_1 r1
	on b.hadm_id=r1.hadm_id 
left join renal_2 r2 
	on b.stay_id=r2.stay_id
left join renal_3 r3
	on b.stay_id=r3.stay_id
) 
, sofa_1 as
( 
select distinct on (stay_id) stay_id  
	, sofa_24hours as d1_sofa 
from mimiciv_derived.sofa 
	where hr < 24 and sofa_24hours is not null 
order by stay_id, hr desc
)
, sofa_2 as
( 
select distinct on (stay_id) stay_id  
	, sofa_24hours as d2_sofa 
from mimiciv_derived.sofa 
	where hr >= 24 and hr < 48 and sofa_24hours is not null
order by stay_id, hr desc 
)
, sofa_3 as
( 
select distinct on (stay_id) stay_id  
	, sofa_24hours as d3_sofa 
from mimiciv_derived.sofa 
	where hr >= 48 and hr < 72 and sofa_24hours is not null
order by stay_id, hr desc 
)
, sofa_4 as
( 
select distinct on (stay_id) stay_id  
	, sofa_24hours as d4_sofa 
from mimiciv_derived.sofa 
	where hr >= 72 and hr < 96 and sofa_24hours is not null
order by stay_id, hr desc 
)
, sofa_5 as
( 
select distinct on (stay_id) stay_id  
	, sofa_24hours as d5_sofa 
from mimiciv_derived.sofa 
	where hr >= 96 and hr < 120 and sofa_24hours is not null
order by stay_id, hr desc 
)
, base_7 as 
(
select b.*
	, weight
	, apsiii 
	, myocardial_infarct, congestive_heart_failure, diabetes_with_cc, charlson_comorbidity_index
	, hx_dialysis, kdigo
	, d1_noradrenalin, d1_phenylephrine, d1_vasopressin, d1_adrenalin 
	, d1_dopamine, d1_dobutamin, d1_milrinone, d1_angiotensin, d1_vasopressor_rate
	, d2_noradrenalin, d2_phenylephrine, d2_vasopressin, d2_adrenalin 
	, d2_dopamine, d2_dobutamin, d2_milrinone, d2_angiotensin, d2_vasopressor_rate
	, d3_noradrenalin, d3_phenylephrine, d3_vasopressin, d3_adrenalin 
	, d3_dopamine, d3_dobutamin, d3_milrinone, d3_angiotensin, d3_vasopressor_rate
	, d4_noradrenalin, d4_phenylephrine, d4_vasopressin, d4_adrenalin 
	, d4_dopamine, d4_dobutamin, d4_milrinone, d4_angiotensin, d4_vasopressor_rate
	, d5_noradrenalin, d5_phenylephrine, d5_vasopressin, d5_adrenalin 
	, d5_dopamine, d5_dobutamin, d5_milrinone, d5_angiotensin, d5_vasopressor_rate
	, max_dobutamin, max_milrinone, max_vasopressor
	, d1_cristalloid_bolus, d1_colloid_bolus, d1_bolus, d1_blood, d1_intake, d1_urine, d1_dialysis, d1_output 
	, d1_fluid_balance 
	, d2_cristalloid_bolus, d2_colloid_bolus, d2_bolus, d2_blood, d2_intake, d2_urine, d2_dialysis, d2_output
	, d2_fluid_balance, d2_cumm_fluid_balance
	, d3_cristalloid_bolus, d3_colloid_bolus, d3_bolus, d3_blood, d3_intake, d3_urine, d3_dialysis, d3_output 
	, d3_fluid_balance, d3_cumm_fluid_balance
	, d4_cristalloid_bolus, d4_colloid_bolus, d4_bolus, d4_blood, d4_intake, d4_urine, d4_dialysis, d4_output 
	, d4_fluid_balance, d4_cumm_fluid_balance
	, d5_cristalloid_bolus, d5_colloid_bolus, d5_bolus, d5_blood, d5_intake, d5_urine, d5_dialysis, d5_output 
	, d5_fluid_balance, d5_cumm_fluid_balance
	, d1_sofa, d2_sofa, d3_sofa, d4_sofa, d5_sofa
	, greatest(d1_sofa, d2_sofa, d3_sofa, d4_sofa, d5_sofa) as d_max_sofa
from base_6 b 
left join public.vasopressor v1
	on b.stay_id = v1.stay_id
left join public.fluid_balance f 
	on b.stay_id = f.stay_id
left join mimiciv_derived.apsiii a  
	on b.stay_id = a.stay_id
left join mimiciv_derived.charlson ch
	on b.hadm_id = ch.hadm_id
left join renal r  
	on b.stay_id = r.stay_id
left join mimiciv_derived.first_day_weight w
	on b.stay_id = w.stay_id
left join sofa_1 s1
	on b.stay_id = s1.stay_id
left join sofa_2 s2
	on b.stay_id = s2.stay_id
left join sofa_3 s3 
	on b.stay_id = s3.stay_id
left join sofa_4 s4 
	on b.stay_id = s4.stay_id
left join sofa_5 s5 
	on b.stay_id = s5.stay_id
)
, base as 
( 
select * 
from base_7 
	where (d1_vasopressor_rate > 0 or d2_vasopressor_rate > 0)
) 
, tte as 
( 
select stay_id 
	, case when tte_offset1 is not null then 1 else 0 end as tte 
	, tte_start1
	,case when tte_offset1 < 0 then 0 else tte_offset1 end as tte_offset 
	,case when tte_offset15 < (5 * 24 * 60) then 15
			when tte_offset14 < (5 * 24 * 60) then 14
			when tte_offset13 < (5 * 24 * 60) then 13
			when tte_offset12 < (5 * 24 * 60) then 12
			when tte_offset11 < (5 * 24 * 60) then 11
			when tte_offset10 < (5 * 24 * 60) then 10
			when tte_offset9 < (5 * 24 * 60) then 9
			when tte_offset8 < (5 * 24 * 60) then 8
			when tte_offset7 < (5 * 24 * 60) then 7
			when tte_offset6 < (5 * 24 * 60) then 6
			when tte_offset5 < (5 * 24 * 60) then 5
			when tte_offset4 < (5 * 24 * 60) then 4
			when tte_offset3 < (5 * 24 * 60) then 3
			when tte_offset2 < (5 * 24 * 60) then 2
			else 1 end as tte_number 
from base 
left join public.tte 
	using (stay_id)
	where tte_offset1 < (5 * 24 * 60) 
) 
, cristalloid as 
(
select stay_id 
	, DATE_PART('day', i.starttime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.starttime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.starttime ::timestamp - icu_intime::timestamp) as infusion_start_offset
    , DATE_PART('day', i.endtime ::timestamp - icu_intime::timestamp) * 24 *60 + 
      DATE_PART('hour', i.endtime ::timestamp - icu_intime::timestamp) * 60 +
      DATE_PART('minute', i.endtime ::timestamp - icu_intime::timestamp) as infusion_end
	, case when amountuom in ('ml','mL', 'cm3') then round (cast (amount as numeric))
			when amountuom in ('L') then round (cast ((1000* amount) as numeric))
			 else null end as amount 
from mimiciv_icu.inputevents i
inner join base
	using (stay_id) 
	where itemid in (225828, 225827, 225158, 225825)
-- 22158 NS, 225825 NSD5 
-- 225828 LR, 225827 D5LR 
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
      DATE_PART('minute', i.endtime ::timestamp - icu_intime::timestamp) as infusion_end
	, round (cast (amount as numeric)) as amount 
from mimiciv_icu.inputevents i
inner join base b
	using (stay_id) 
	where itemid in (220864, 225795, 225174) ---- 220864 alb 5%, 225795 Dextran 40, 225174 hetastarch
	and amount > 100
	and (rateuom = 'mL/min' or rate > 250) 
)
, bolus as 
(
select stay_id 
	, infusion_start_offset 
	, min(coalesce (c.infusion_end, a.infusion_end)) as infusion_end_offset 
	, coalesce (sum(c.amount),0) as cristalloid_bolus
	, coalesce (sum(a.amount),0) as albumin_bolus 
from cristalloid c 
full outer join albumin a
	using (stay_id, infusion_start_offset) 
	group by stay_id, infusion_start_offset
) 
, dobut_1 as
( 
select stay_id 
	, starttime 
	, min(DATE_PART('day', starttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - icu_intime::timestamp)) as dobutamin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - icu_intime::timestamp)) as dobutamin_end_offset
	, max(vaso_rate) as dobutamin_rate
	, rank () over (partition by stay_id order by starttime) as rn 
from mimiciv_derived.dobutamine d
inner join base 
	using (stay_id)
	where vaso_rate > 0 
group by stay_id, starttime 
order by stay_id, starttime 
)
, dobut as 
(
select stay_id 
	, dobutamin_begin_offset, dobutamin_end_offset  
	, round (cast (dobutamin_rate as numeric), 1) as dobutamin_rate  
	, case when rn = 1 then 'start'
		when rn > 1 and dobutamin_begin_offset > (lag(dobutamin_end_offset) over (partition by stay_id order by rn) + 60) then 'start' 
--		when rn > 1 and dobutamin_rate > (lag(dobutamin_rate) over (partition by stay_id order by rn) + 2) then 'increase'  
--		when rn > 1 and dobutamin_rate < (lag(dobutamin_rate) over (partition by stay_id order by rn) - 2) then 'decrease'
		else null end as dobutamin_change -- else 'unchanged' end as dobutamin_change
	, case when lead(dobutamin_begin_offset) over (partition by stay_id order by rn) is null 
			or dobutamin_end_offset + 60 < lead(dobutamin_begin_offset) over (partition by stay_id order by rn)
				then 1 else 0 end as dobutamin_end
	, rn 
from dobut_1 
order by stay_id, rn
) 
, milrinone_1 as
( 
select stay_id 
	, starttime 
	, min(DATE_PART('day', starttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - icu_intime::timestamp)) as milrinone_begin_offset
    , min(DATE_PART('day', endtime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - icu_intime::timestamp)) as milrinone_end_offset
	, max(rate) as milrinone_rate
	, rank () over (partition by stay_id order by starttime) as rn 
from mimiciv_icu.inputevents i 
inner join base 
	using (stay_id)
	where itemid = 221986 -- milrinone
	and rate > 0 
group by stay_id, starttime 
order by stay_id, starttime 
)
, milrinone as 
(
select stay_id 
	, milrinone_begin_offset, milrinone_end_offset  
	, round (cast (milrinone_rate as numeric), 2) as milrinone_rate  
	, case when rn = 1 then 'start'
		when rn > 1 and milrinone_begin_offset > (lag(milrinone_end_offset) over (partition by stay_id order by rn) + 60) then 'start' 
--		when rn > 1 and milrinone_rate > (lag(milrinone_rate) over (partition by stay_id order by rn) + 0.1) then 'increase'  
--		when rn > 1 and milrinone_rate < (lag(milrinone_rate) over (partition by stay_id order by rn) - 0.1) then 'decrease'
		else null end as milrinone_change -- else 'unchanged' end as milrinone_change
	, case when lead(milrinone_begin_offset) over (partition by stay_id order by rn) is null 
			or milrinone_end_offset + 60 < lead(milrinone_begin_offset) over (partition by stay_id order by rn)
				then 1 else 0 end as milrinone_end
	, rn 
from milrinone_1 
order by stay_id, rn
) 
, adrenaline_1 as
( 
select stay_id 
	, starttime 
	, min(DATE_PART('day', starttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - icu_intime::timestamp)) as adrenaline_begin_offset
    , min(DATE_PART('day', endtime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - icu_intime::timestamp)) as adrenaline_end_offset
	, max(vaso_rate) as adrenaline_rate
	, rank () over (partition by stay_id order by starttime) as rn 
from mimiciv_derived.epinephrine d
inner join base 
	using (stay_id)
	where vaso_rate > 0 
group by stay_id, starttime 
order by stay_id, starttime 
)
, adrenaline as 
(
select stay_id 
	, adrenaline_begin_offset, adrenaline_end_offset  
	, round (cast (adrenaline_rate as numeric), 2) as adrenaline_rate  
	, case when rn = 1 then 'start'
		when rn > 1 and adrenaline_begin_offset > (lag(adrenaline_end_offset) over (partition by stay_id order by rn) + 60) then 'start' 
	--	when rn > 1 and adrenaline_rate > (lag(adrenaline_rate) over (partition by stay_id order by rn) + 0.02) then 'increase'  
	--	when rn > 1 and adrenaline_rate < (lag(adrenaline_rate) over (partition by stay_id order by rn) - 0.02) then 'decrease'
	else null end as adrenaline_change -- 	else 'unchanged' end as adrenaline_change
	, case when lead(adrenaline_begin_offset) over (partition by stay_id order by rn) is null 
			or adrenaline_end_offset + 60 < lead(adrenaline_begin_offset) over (partition by stay_id order by rn) 
				then 1 else 0 end as adrenaline_end
	, rn 
from adrenaline_1 
order by stay_id, rn
) 
, vasopressin_1 as
( 
select stay_id 
	, starttime 
	, min(DATE_PART('day', starttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', starttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', starttime::timestamp - icu_intime::timestamp)) as vasopressin_begin_offset
    , min(DATE_PART('day', endtime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', endtime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', endtime::timestamp - icu_intime::timestamp)) as vasopressin_end_offset
	, max(vaso_rate) as vasopressin_rate 
	, rank () over (partition by stay_id order by starttime) as rn 
from mimiciv_derived.vasopressin d
inner join base 
	using (stay_id)
	where vaso_rate > 0 and vaso_rate <= 6 
group by stay_id, starttime 
order by stay_id, starttime 
)
, vasopressin as 
(
select stay_id 
	, vasopressin_begin_offset, vasopressin_end_offset  
	, round (cast (vasopressin_rate as numeric), 1) as vasopressin_rate 
	, case when rn = 1 then 'start'
		when rn > 1 and vasopressin_begin_offset > (lag(vasopressin_end_offset) over (partition by stay_id order by rn) + 60) then 'start' 
--		when rn > 1 and vasopressin_rate > (lag(vasopressin_rate) over (partition by stay_id order by rn) + 1) then 'increase'  
--		when rn > 1 and vasopressin_rate < (lag(vasopressin_rate) over (partition by stay_id order by rn) - 1) then 'decrease'
		else null end as vasopressin_change --else 'unchanged' end as vasopressin_change
	, case when lead(vasopressin_begin_offset) over (partition by stay_id order by rn) is null 
			or vasopressin_end_offset + 60 < lead(vasopressin_begin_offset) over (partition by stay_id order by rn) 
				then 1 else 0 end as vasopressin_end
	, rn 
from vasopressin_1 
order by stay_id, rn 
) 
, diuretic as
( 
select b.stay_id 
	, p.starttime 
	, min(DATE_PART('day', p.starttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', p.starttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', p.starttime::timestamp - icu_intime::timestamp)) as diuretic_offset
	, max(round (case when p2.frequency in ('INFUSION', 'IV DRIP')and p.prod_strength in ('100mg/10mL Vial', '2.5mg/10mL Vial') 
				then 0 else 
					case when drug in ('Furo','Furos','furose','Furosemi','Furosemid','furosemide','Furosemide'
										,'Furosemide ','Furosemide 100mg/10mL 10mL VIAL','Furosemide 20mg/2mL 2mL VIAL'
										,'Furosemide 40mg/4mL 4mL VIAL','Furosemide-Heart Failure','Furosemide in 0.9% Sodium Chloride'
										,'Furosemide (Latex Free)','Furosemide Solution', 'Lasix', 'LaSIX'
										,'torsemide','Torsemide','Torsemide ') then cast (p.dose_val_rx as numeric)
					when drug in ('Bume','Bumet','Bumeta','Bumetan','Bumetanid','Bumetanide','bumex') then 20 * cast (p.dose_val_rx as numeric)
					when drug in ('Ethacrynate Sodium', 'Ethacrynic Acid') then 0.4 * cast (p.dose_val_rx as numeric)
						else null end end)) as diuretic_amount
	, max (case when p2.frequency in ('INFUSION', 'IV DRIP') 
			and p.prod_strength in ('100mg/10mL Vial', '2.5mg/10mL Vial') then 1 else 0 end) as diuretic_infusion 
	, rank () over (partition by stay_id order by p.starttime) as rn 
from base b
inner join mimiciv_hosp.prescriptions p
	on b.hadm_id = p.hadm_id 
inner join mimiciv_hosp.pharmacy p2
	on p.pharmacy_id = p2.pharmacy_id 
	where p.drug in (
		 'Furo','Furos','furose','Furosemi','Furosemid','furosemide','Furosemide'
		,'Furosemide ','Furosemide 100mg/10mL 10mL VIAL','Furosemide 20mg/2mL 2mL VIAL'
		,'Furosemide 40mg/4mL 4mL VIAL','Furosemide-Heart Failure','Furosemide in 0.9% Sodium Chloride'
		,'Furosemide (Latex Free)','Furosemide Solution', 'Lasix', 'LaSIX'
		---- torsemide 
		,'torsemide','Torsemide','Torsemide '
		---- bumetanide 
		,'Bume','Bumet','Bumeta','Bumetan','Bumetanid','Bumetanide','bumex'
		-----  ethacrynate 
		,'Ethacrynate Sodium', 'Ethacrynic Acid')
	and p.route in ('IV DRIP','IV')
	and dose_val_rx ~ '^[-]?[0-9]+[.]?[0-9]*$'
	and dose_val_rx not in ('-','.')
group by stay_id, p.starttime 
)
, betablocker as
( 
select b.stay_id 
	, p.starttime 
	, min(DATE_PART('day', p.starttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', p.starttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', p.starttime::timestamp - icu_intime::timestamp)) as betablocker_offset
	, max(case when drug in (
			'Esmolol','Esmolol (0.9% NaCl)','Esmolol in Saline (Iso-osm)'
			'Metoprolol','metoCLOPramide','Metoprolol Tartrate','metoprolol tartrate') then 1
		else 0 end) as betablocker 
	, max(case when drug in ('Diltia','Diltiaz','Diltiazem','Verapamil','Verapamil HCl') then 1 
		else 0 end) as calcium_antagonist
	, rank () over (partition by stay_id order by p.starttime) as rn 
from base b
inner join mimiciv_hosp.prescriptions p
	on b.hadm_id = p.hadm_id 
inner join mimiciv_hosp.pharmacy p2
	on p.pharmacy_id = p2.pharmacy_id 
	where p.drug in (
			'Esmolol','Esmolol (0.9% NaCl)','Esmolol in Saline (Iso-osm)'
			'Metoprolol','metoCLOPramide','Metoprolol Tartrate','metoprolol tartrate'
			'Diltia','Diltiaz','Diltiazem'
			'Verapamil','Verapamil HCl'
			)
	and p.route in ('IV DRIP','IV')
group by stay_id, p.starttime 
)
, CO as 
(
select distinct on (v.stay_id) stay_id 
	, charttime as co_time  
	, DATE_PART('day', charttime::timestamp - icu_intime::timestamp) * 24 *60 + 
       DATE_PART('hour', charttime::timestamp - icu_intime::timestamp) * 60 +
       DATE_PART('minute', charttime::timestamp - icu_intime::timestamp) as co_offset
    , 1 as co 
from base b 
inner join public.vitals v 
	using (stay_id)
	where 
	svo2 > 0 or pap_d > 0 or pap_s >0 or pap_m >0 
	or pcwp > 0 or co_cont > 0 or co_therm > 0 
	or cardiac_function_index > 0 or  picco_ci > 0 or picco_co > 0 
	or ELWI > 0 or  GEDI > 0 or  ITBVI > 0 or  picco_SVI > 0 or picco_SVRI > 0 or picco_SVV > 0 
order by v.stay_id, v.rn 
) 
, combine_tte as 
( 
select t.* 
	, tte_cristalloid_bolus, tte_albumin_bolus 
	, dobutamin_begin_offset as tte_dobutamin_begin_offset, dobutamin_rate as tte_dobutamin_rate, dobutamin_change as tte_dobutamin_change
	, milrinone_begin_offset as tte_milrinone_begin_offset, milrinone_rate as tte_milrinone_rate, milrinone_change as tte_milrinone_change
	, adrenaline_begin_offset as tte_adrenaline_begin_offset, adrenaline_rate as tte_adrenaline_rate
	, adrenaline_change as tte_adrenaline_change
	, case when dobutamin_change is null  and  milrinone_change is null and adrenaline_change is null then null 
		when dobutamin_change = 'start' or milrinone_change = 'start' or adrenaline_change = 'start' then 'start'
--		when dobutamin_change = 'increase' or milrinone_change = 'increase' or adrenaline_change = 'increase' then 'increase'
--		when dobutamin_change = 'decrease' or milrinone_change = 'decrease' or adrenaline_change = 'decrease' then 'decrease'
		when dobutamin_change = 'stop' or milrinone_change = 'stop' or adrenaline_change = 'stop' then 'stop'
			else 'unchanged' end as tte_inotrope 
	, vasopressin_begin_offset as tte_vasopressin_begin_offset, vasopressin_rate as tte_vasopressin_rate, vasopressin_change as tte_vasopressin_change
	, tte_diuretic_offset, tte_diuretic_amount, tte_diuretic_infusion  
	, tte_betablocker_offset, tte_betablocker, tte_calcium_antagonist 
	, co_offset, case when co is null then 0 else co end as tte_co 
from tte t
left join (
		select stay_id  
				, sum(cristalloid_bolus) as tte_cristalloid_bolus 
				, sum(albumin_bolus) as tte_albumin_bolus 
		from tte 
		inner join bolus
			using (stay_id)
			where (
				tte_offset <= infusion_start_offset
				and tte_offset > infusion_start_offset - 4 * 60)
			group by stay_id 
		) b 
		on b.stay_id = t.stay_id 
left join (
		select stay_id  
				, dobutamin_begin_offset, dobutamin_rate 
				, case when tte_offset > dobutamin_begin_offset then 'stop' else dobutamin_change end as dobutamin_change
				, rank () over (partition by stay_id order by dobutamin_begin_offset) as bn 
		from tte 
		inner join dobut
			using (stay_id)
			where (
				tte_offset > dobutamin_begin_offset 
				and tte_offset <= dobutamin_end_offset 
				and tte_offset + (4 * 60) >= dobutamin_end_offset
				and dobutamin_end = 1) 
				or (
				tte_offset <= dobutamin_begin_offset
				and tte_offset > dobutamin_begin_offset - 4 * 60
				and dobutamin_change is not null) 
		) d 
		on d.stay_id = t.stay_id and bn =1 
left join (
		select stay_id  
				, milrinone_begin_offset, milrinone_rate
				, case when tte_offset > milrinone_begin_offset then 'stop' else milrinone_change end as milrinone_change
				, rank () over (partition by stay_id order by milrinone_begin_offset) as cn 
		from tte 
		inner join milrinone
			using (stay_id)
			where (
				tte_offset > milrinone_begin_offset 
				and tte_offset <= milrinone_end_offset 
				and tte_offset + (4 * 60) >= milrinone_end_offset
				and milrinone_end = 1) 
				or (
				tte_offset <= milrinone_begin_offset
				and tte_offset > milrinone_begin_offset - 4 * 60
				and milrinone_change is not null) 
		) m 
		on m.stay_id = t.stay_id and cn = 1 
left join (
		select stay_id  
				, adrenaline_begin_offset, adrenaline_rate
				, case when tte_offset > adrenaline_begin_offset then 'stop' else adrenaline_change end as adrenaline_change
				, rank () over (partition by stay_id order by adrenaline_begin_offset) as dn 
		from tte 
		inner join adrenaline
			using (stay_id)
			where (
				tte_offset > adrenaline_begin_offset 
				and tte_offset <= adrenaline_end_offset 
				and tte_offset + (4 * 60) >= adrenaline_end_offset
				and adrenaline_end = 1) 
				or (
				tte_offset <= adrenaline_begin_offset
				and tte_offset > adrenaline_begin_offset - 4 * 60
				and adrenaline_change is not null) 
		) adr 
		on adr.stay_id = t.stay_id and dn = 1 
left join (
		select stay_id  
				, vasopressin_begin_offset, vasopressin_rate
				, case when tte_offset > vasopressin_begin_offset then 'stop' else vasopressin_change end as vasopressin_change
				, rank () over (partition by stay_id order by vasopressin_begin_offset) as en 
		from tte 
		inner join vasopressin
			using (stay_id)
			where (
				tte_offset > vasopressin_begin_offset 
				and tte_offset <= vasopressin_end_offset
				and tte_offset + (4 * 60) >= vasopressin_end_offset
				and vasopressin_end = 1) 
				or (
				tte_offset <= vasopressin_begin_offset
				and tte_offset > vasopressin_begin_offset - 4 * 60
				and vasopressin_change is not null) 
		) v
		on v.stay_id = t.stay_id and en =1 
left join (
		select stay_id  
				, min (diuretic_offset) as tte_diuretic_offset
				, sum (diuretic_amount) as tte_diuretic_amount
				, max (diuretic_infusion) as tte_diuretic_infusion 
--				, rank () over (partition by stay_id order by diuretic_bolus_offset) as fn 
		from tte 
		inner join diuretic
			using (stay_id)
			where (
				tte_offset <= diuretic_offset
				and tte_offset > diuretic_offset - 4 * 60)
		group by stay_id 
		) diu
		on diu.stay_id = t.stay_id 
left join (
		select stay_id  
				, min (betablocker_offset) as tte_betablocker_offset
				, max (betablocker) as tte_betablocker
				, max (calcium_antagonist) as tte_calcium_antagonist
--				, rank () over (partition by stay_id order by diuretic_bolus_offset) as fn 
		from tte 
		inner join betablocker
			using (stay_id)
			where (
				tte_offset <= betablocker_offset
				and tte_offset > betablocker_offset - 4 * 60)
		group by stay_id 
		) beta
		on beta.stay_id = t.stay_id 
left join ( 
		select stay_id 
			, co_offset 
			, co 
		from tte 
		inner join co 
			using (stay_id)
			where tte_offset <= co_offset
				and tte_offset > co_offset - 4 * 60
		) co 
	on co.stay_id = t.stay_id
order by stay_id 
)
-------------------------------------------
--------- Mechanical ventilation -> trachy, invasive and NIV are counted as ventilation 
------------------------------------
, vent as 
(
select stay_id 
		, DATE_PART('day', starttime::timestamp - intime_hr::timestamp) * 24 *60 + 
       			DATE_PART('hour', starttime::timestamp - intime_hr::timestamp) * 60 +
       			DATE_PART('minute', starttime::timestamp - intime_hr::timestamp) as vent_offset_start
       	, DATE_PART('day', endtime::timestamp - intime_hr::timestamp) * 24 *60 + 
       			DATE_PART('hour', endtime::timestamp - intime_hr::timestamp) * 60 +
       			DATE_PART('minute', endtime::timestamp - intime_hr::timestamp) as vent_offset_end
       	, case when ventilation_status in ('Tracheostomy','InvasiveVent','NonInvasiveVent') then 1 else 0 end as vent 
from mimiciv_derived.ventilation 
inner join mimiciv_derived.icustay_times it 
	using (stay_id)
)
,block_1 as 
(
select stay_id 
	, array(
		select * 
		from generate_series (
			0,round(cast(
				DATE_PART('day', outtime_hr::timestamp - intime_hr::timestamp) * 24 *60 + 
       			DATE_PART('hour', outtime_hr::timestamp - intime_hr::timestamp) * 60 +
       			DATE_PART('minute', outtime_hr::timestamp - intime_hr::timestamp) as numeric) / (4 * 60))
       			)) as block_number    
from mimiciv_derived.icustay_times it 
) 
, block_2 as 
(
select stay_id
	, block  
	, (block * (4 * 60))-(4 * 60)  as minute_start 
	, case when block = 0 then 60 else (block * (4 * 60)) end as minute_end 
from block_1 
cross join unnest(block_number) as block
)
, block as 
(
select base.* 
	, block
	, minute_start, minute_end 
from block_2
inner join base 
	using (stay_id) 
	where minute_start < (5 * 24 * 60) 
order by stay_id, block 
)
, combine as 
( 
select b.*
	, block_hour 
	, case when block_hour >= 6 and block_hour < 16 then 'day'
			when block_hour >= 16 and block_hour < 22 then 'evening'
			else 'night' end as block_period
	, tte_offset, tte, tte_number, to_char(tte_start1, 'HH24:MI') as tte_time 
	, tte_cristalloid_bolus, tte_albumin_bolus
	, tte_dobutamin_rate, tte_dobutamin_change, tte_milrinone_rate, tte_milrinone_change
	, tte_adrenaline_rate, tte_adrenaline_change,tte_inotrope, tte_vasopressin_change, tte_diuretic_amount, tte_diuretic_infusion  
	, tte_betablocker_offset, tte_betablocker, tte_calcium_antagonist, tte_co
	, coalesce (vasopressor_rate, 0) as vasopressor_rate
	, case when tte_inotrope = 'start' then 0 
			when tte_inotrope in ('stop', 'increase', 'unchanged') then 1 
			else coalesce (inotrope, 0) end as inotrope 
	, round (coalesce (cristalloid_bolus ,0) + coalesce (albumin_bolus,0)) as fluid_bolus
	, fluid_balance 
	, sofa 
	, case when vent is not null then vent else 0 end as vent
	, case when sf_ratio is not null then sf_ratio else 475 end as sf_ratio
	, dobutamin_rate, dobutamin_change
	, milrinone_rate, milrinone_change
	, adrenaline_rate, adrenaline_change
	, case when dobutamin_change is null  and  milrinone_change is null and adrenaline_change is null then null 
		when dobutamin_change = 'start' or milrinone_change = 'start' or adrenaline_change = 'start' then 'start'
	--	when dobutamin_change = 'increase' or milrinone_change = 'increase' or adrenaline_change = 'increase' then 'increase'
	--	when dobutamin_change = 'decrease' or milrinone_change = 'decrease' or adrenaline_change = 'decrease' then 'decrease'
		when dobutamin_change = 'stop' or milrinone_change = 'stop' or adrenaline_change = 'stop' then 'stop'
			else 'unchanged' end as inotrope_change 
	, vasopressin_rate, vasopressin_change
	, diuretic_amount, diuretic_infusion  
	, betablocker, calcium_antagonist 
	, case when co is null then 0 else co end as co 
from block b  
left join ( 
	select block  
		, t.* 
	from combine_tte t
	inner join block
		using (stay_id) 
		where tte_offset >= minute_start
			and case when tte_offset < 60 then 61 else tte_offset end < minute_end
		) tte 
	on b.stay_id = tte.stay_id and b.block = tte.block  
left join ( 
	select stay_id 
		, block 
		, round (max(vasopressor_rate), 2) as vasopressor_rate  
		, max(case when dobutamin > 0 or milrinone > 0 or adrenalin > 0 then 1 else 0 end) as inotrope
	from block  
	inner join public.vasopressor_hourly 
		using (stay_id)
	where (hr * 60) >= minute_start
		and (hr * 60) < minute_end 
	group by stay_id, block 
	order by stay_id, block
	) v 
	on b.stay_id = v.stay_id and b.block = v.block 
left join (
	select stay_id 
		, block 
		, sum (cristalloid_bolus) as cristalloid_bolus 
		, sum (albumin_bolus) as albumin_bolus 
	from block 
	inner join bolus 
	using (stay_id)
		where infusion_start_offset >= minute_start
		and infusion_start_offset < minute_end
	group by stay_id, block
	order by stay_id, block
	) f
	on b.stay_id = f.stay_id and b.block = f.block
left join (
	select stay_id 
		, block 
		, est_cumm_hourly_balance as fluid_balance 
	from block 
	inner join public.fluid_hourly  
	using (stay_id)
		where (hr * 60) >= minute_start + (3 * 60)
		and (hr * 60) < minute_end
	) t 
	on b.stay_id = t.stay_id and b.block = t.block
left join ( 
	select stay_id  
		, block 
		, case when block = 0 then coagulation_24hours + liver_24hours + cns_24hours + renal_24hours
				when lead(sofa_24hours, 3) over (partition by stay_id order by hr) is not null then	
--			 		lead(respiration_24hours, 3) over (partition by stay_id order by hr) +
			 		lead(coagulation_24hours, 3) over (partition by stay_id order by hr) +  
			 		lead(liver_24hours, 3) over (partition by stay_id order by hr) +
			 		lead(cns_24hours, 3) over (partition by stay_id order by hr) +
			 		lead(renal_24hours, 3) over (partition by stay_id order by hr)
			 	when lead(sofa_24hours, 2) over (partition by stay_id order by hr) is not null then	
--			 		lead(respiration_24hours, 2) over (partition by stay_id order by hr) 
			 		lead(coagulation_24hours, 2) over (partition by stay_id order by hr) + 
			 		lead(liver_24hours, 2) over (partition by stay_id order by hr) + 
			 		lead(cns_24hours, 2) over (partition by stay_id order by hr) +
			 		lead(renal_24hours, 2) over (partition by stay_id order by hr) 
			 	when lead(sofa_24hours) over (partition by stay_id order by hr) is not null then 
--			 		lead(respiration_24hours) over (partition by stay_id order by hr) 
			 		lead(coagulation_24hours) over (partition by stay_id order by hr) + 
			 		lead(liver_24hours) over (partition by stay_id order by hr) +
			 		lead(cns_24hours) over (partition by stay_id order by hr) +
			 		lead(renal_24hours) over (partition by stay_id order by hr)
				else coagulation_24hours + liver_24hours + cns_24hours + renal_24hours end as sofa
		, case when block = 0 then DATE_PART('hour', starttime::timestamp - interval '3 hour') else  DATE_PART('hour', starttime::timestamp) end as block_hour 
	from mimiciv_derived.sofa 
	inner join block 
		using (stay_id)
		where case when block = 0 then 0 else minute_start end <= (hr * 60) 
		and case when block = 0 then 60 else minute_end - (3 * 60) end > (hr * 60)
		) s 
		on b.stay_id = s.stay_id and b.block = s.block
left join (
	select stay_id 
		, block 
		, max(vent) as vent  
	from block 
	inner join vent
	using (stay_id)
		where vent_offset_end >= minute_start 
		and vent_offset_start < minute_end
	group by stay_id, block 
	) vent
	on b.stay_id = vent.stay_id and b.block = vent.block
left join ( 
	select stay_id
		, block 
		, min (sf_ratio) as sf_ratio 
	from block 
	inner join public.oxygen_treat 
		using (stay_id)
		where (hr * 60)  >= minute_start
		and (hr * 60) < minute_end
	group by stay_id, block 
	) sf 
	on b.stay_id = sf.stay_id and b.block = sf.block 
left join (
		select stay_id, block  
				, dobutamin_rate
				, case when minute_start > dobutamin_begin_offset then 'stop' else dobutamin_change end as dobutamin_change
				, rank () over (partition by stay_id, block order by dobutamin_begin_offset) as bn 
		from block
		inner join dobut
			using (stay_id)
			where (
				minute_start > dobutamin_begin_offset 
				and minute_start <= dobutamin_end_offset 
				and minute_end >= dobutamin_end_offset
				and dobutamin_end = 1) 
				or (
				minute_start <= dobutamin_begin_offset
				and minute_end > dobutamin_begin_offset
				and dobutamin_change is not null) 
		) d 
		on b.stay_id = d.stay_id and b.block = d.block and bn =1 
left join (
		select stay_id, block   
				, milrinone_rate
				, case when minute_start > milrinone_begin_offset then 'stop' else milrinone_change end as milrinone_change
				, rank () over (partition by stay_id, block order by milrinone_begin_offset) as cn 
		from block 
		inner join milrinone
			using (stay_id)
			where (
				minute_start > milrinone_begin_offset 
				and minute_start <= milrinone_end_offset 
				and minute_end >= milrinone_end_offset
				and milrinone_end = 1) 
				or (
				minute_start <= milrinone_begin_offset
				and minute_end > milrinone_begin_offset
				and milrinone_change is not null)
		) m 
		on b.stay_id = m.stay_id and b.block = m.block and cn = 1 
left join (
		select stay_id, block   
				, adrenaline_rate
				, case when minute_start > adrenaline_begin_offset then 'stop' else adrenaline_change end as adrenaline_change
				, rank () over (partition by stay_id, block order by adrenaline_begin_offset) as dn 
		from block
		inner join adrenaline
			using (stay_id)
			where (
				minute_start > adrenaline_begin_offset 
				and minute_start <= adrenaline_end_offset
				and minute_end >= adrenaline_end_offset
				and adrenaline_end = 1) 
				or (
				minute_start <= adrenaline_begin_offset
				and minute_end > adrenaline_begin_offset
				and adrenaline_change is not null) 
		) adr 
		on b.stay_id = adr.stay_id and b.block = adr.block and dn = 1
left join (
		select stay_id, block  
				, vasopressin_rate
				, case when minute_start > vasopressin_begin_offset then 'stop' else vasopressin_change end as vasopressin_change
				, rank () over (partition by stay_id, block order by vasopressin_begin_offset) as en 
		from block
		inner join vasopressin
			using (stay_id)
			where (
				minute_start > vasopressin_begin_offset 
				and minute_start <= vasopressin_end_offset
				and minute_end >= vasopressin_end_offset
				and vasopressin_end = 1) 
				or (
				minute_start <= vasopressin_begin_offset
				and minute_end > vasopressin_begin_offset
				and vasopressin_change is not null) 
		) vaso
		on b.stay_id = vaso.stay_id and b.block = vaso.block and en =1 
left join (
		select stay_id, block   
				, sum (diuretic_amount) as diuretic_amount
				, max (diuretic_infusion) as diuretic_infusion 
--				, rank () over (partition by stay_id order by diuretic_bolus_offset) as fn 
		from block
		inner join diuretic
			using (stay_id)
			where (
				minute_start <= diuretic_offset
				and minute_end > diuretic_offset)
		group by stay_id, block  
		) diu
		on b.stay_id = diu.stay_id and b.block = diu.block 
left join (
		select stay_id, block   
				, max (betablocker) as betablocker
				, max (calcium_antagonist) as calcium_antagonist
		from block
		inner join betablocker
			using (stay_id)
			where (
				minute_start <= betablocker_offset
				and minute_end > betablocker_offset)
		group by stay_id, block 
		) beta
		on b.stay_id = beta.stay_id and b.block = beta.block 
left join ( 
		select stay_id, block  
			, co 
		from block
		inner join co 
			using (stay_id)
			where minute_start <= co_offset
				and minute_end > co_offset 
		) co 
	on b.stay_id = co.stay_id and b.block = co.block
)
select *
from combine
order by stay_id, block
	 
	












