
DROP TABLE IF EXISTS public.vasopressor CASCADE;
CREATE TABLE public.vasopressor as

with d1 as 
(
select stay_id 
	, max (round (noradrenalin, 2)) as d1_noradrenalin 
	, max (round (phenylephrine, 1)) as d1_phenylephrine  
	, max (round (vasopressin, 1)) as d1_vasopressin 
	, max (round (adrenalin, 2)) as d1_adrenalin 
	, max (round (dopamine, 1)) as d1_dopamine 
	, max (round (dobutamin, 1)) as d1_dobutamin  
	, max (round (milrinone, 3)) as d1_milrinone 
	, max (round (angiotensin, 1)) as d1_angiotensin 
	, max (round (vasopressor_rate, 2)) as d1_vasopressor_rate
from public.vasopressor_hourly
	where hr < 24 
group by stay_id 
) 
, d2 as 
(
select stay_id 
	, max (round (noradrenalin, 2)) as d2_noradrenalin 
	, max (round (phenylephrine, 1)) as d2_phenylephrine  
	, max (round (vasopressin, 1)) as d2_vasopressin 
	, max (round (adrenalin, 2)) as d2_adrenalin 
	, max (round (dopamine, 1)) as d2_dopamine 
	, max (round (dobutamin, 1)) as d2_dobutamin  
	, max (round (milrinone, 3)) as d2_milrinone 
	, max (round (angiotensin, 1)) as d2_angiotensin 
	, max (round (vasopressor_rate, 2)) as d2_vasopressor_rate
from public.vasopressor_hourly
	where hr >= 24
		and hr < 48 
group by stay_id
) 
, d3 as 
(
select stay_id 
	, max (round (noradrenalin, 2)) as d3_noradrenalin 
	, max (round (phenylephrine, 1)) as d3_phenylephrine  
	, max (round (vasopressin, 1)) as d3_vasopressin 
	, max (round (adrenalin, 2)) as d3_adrenalin 
	, max (round (dopamine, 1)) as d3_dopamine 
	, max (round (dobutamin, 1)) as d3_dobutamin  
	, max (round (milrinone, 3)) as d3_milrinone 
	, max (round (angiotensin, 1)) as d3_angiotensin 
	, max (round (vasopressor_rate, 2)) as d3_vasopressor_rate
from public.vasopressor_hourly
	where hr >= 48 
		and hr < 72
group by stay_id
) 
, d4 as 
(
select stay_id 
	, max (round (noradrenalin, 2)) as d4_noradrenalin 
	, max (round (phenylephrine, 1)) as d4_phenylephrine  
	, max (round (vasopressin, 1)) as d4_vasopressin 
	, max (round (adrenalin, 2)) as d4_adrenalin 
	, max (round (dopamine, 1)) as d4_dopamine 
	, max (round (dobutamin, 1)) as d4_dobutamin  
	, max (round (milrinone, 3)) as d4_milrinone 
	, max (round (angiotensin, 1)) as d4_angiotensin 
	, max (round (vasopressor_rate, 2)) as d4_vasopressor_rate
from public.vasopressor_hourly
	where hr >= 72
		and hr < 96
group by stay_id
) 
, d5 as 
(
select stay_id 
	, max (round (noradrenalin, 2)) as d5_noradrenalin 
	, max (round (phenylephrine, 1)) as d5_phenylephrine  
	, max (round (vasopressin, 1)) as d5_vasopressin 
	, max (round (adrenalin, 2)) as d5_adrenalin 
	, max (round (dopamine, 1)) as d5_dopamine 
	, max (round (dobutamin, 1)) as d5_dobutamin  
	, max (round (milrinone, 3)) as d5_milrinone 
	, max (round (angiotensin, 1)) as d5_angiotensin 
	, max (round (vasopressor_rate, 2)) as d5_vasopressor_rate
from public.vasopressor_hourly
	where hr >= 96
		and hr < 120
group by stay_id
) 
, combine as 
( 
select d1.stay_id 
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
	, greatest(d1_dobutamin, d2_dobutamin, d3_dobutamin, d4_dobutamin, d5_dobutamin) as max_dobutamin
	, greatest(d1_milrinone, d2_milrinone, d3_milrinone, d4_milrinone, d5_milrinone) as max_milrinone
	, greatest(d1_vasopressor_rate, d2_vasopressor_rate, d3_vasopressor_rate, d4_vasopressor_rate, d5_vasopressor_rate) as max_vasopressor
from d1 
left join d2 
	on d1.stay_id = d2.stay_id 
left join d3
	on d1.stay_id = d3.stay_id
left join d4
	on d1.stay_id = d4.stay_id
left join d5
	on d1.stay_id = d5.stay_id
)
select * 
from combine;

























