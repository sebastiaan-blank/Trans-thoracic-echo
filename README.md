# Trans-thoracic-echo
Project evaluating the use of TTE in septic shock 
Data was extracted from MIMIC IV version 2.0 loaded in a postgresql server (ubuntu) 
-------------------------
To extract the cohort you need the MIMIC IV derived tables:
See Johnson AE, Stone DJ, Celi LA, et al.: The MIMIC Code Repository: enabling reproducibility in critical care research. J Am Med Inform Assoc JAMIA 2018; 25:32–39
These scripts (and instructions) can be found here here: https://github.com/MIT-LCP/mimic-iv/tree/master/concepts
--- 
First run the vasopressor_hourly, fluid_hourly, vasopressor and fluid_balance scripts 
These scripts will create tables in public/table_name needed for extraction
Note the vasopressor_hourly and fluid_hourly scripts can take a while to run depending on your equipment 
---- 
Finally run the cohort_tte scripts to extract the final cohort with all variables. 
The data is extracted as a longitudinal cohort with multiple rows per patient, each row corresponding a 4 hour block
Maximum of 30 block for an individual patient
------
