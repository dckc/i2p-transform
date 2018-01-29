--------------------------------------------------------------------------------
-- MED_ADMIN
--------------------------------------------------------------------------------

BEGIN
PMN_DROPSQL('DROP TABLE med_admin');
END;
/
CREATE TABLE med_admin(
    MEDADMINID varchar(50) primary key,
    PATID varchar(50) NOT NULL,
    ENCOUNTERID varchar(50) NULL,
    PRESCRIBINGID varchar(50) NULL,
    MEDADMIN_PROVIDERID varchar(50) NULL,
    MEDADMIN_START_DATE date NOT NULL,
    MEDADMIN_START_TIME varchar(5) NULL,
    MEDADMIN_STOP_DATE date NULL,
    MEDADMIN_STOP_TIME varchar(5) NULL,
    MEDADMIN_TYPE varchar(2) NULL,
    MEDADMIN_CODE varchar(50) NULL,
    MEDADMIN_DOSE_ADMIN NUMBER(18, 2) NULL, -- (8,0)
    MEDADMIN_DOSE_ADMIN_UNIT varchar(50) NULL,
    MEDADMIN_ROUTE varchar(50) NULL,
    MEDADMIN_SOURCE varchar(2) NULL,
    RAW_MEDADMIN_MED_NAME varchar(50) NULL,
    RAW_MEDADMIN_CODE varchar(50) NULL,
    RAW_MEDADMIN_DOSE_ADMIN varchar(50) NULL,
    RAW_MEDADMIN_DOSE_ADMIN_UNIT varchar(50) NULL,
    RAW_MEDADMIN_ROUTE varchar(50) NULL
)
/
BEGIN
PMN_DROPSQL('DROP sequence med_admin_seq');
END;
/
create sequence med_admin_seq
/
create or replace trigger med_admin_trg
before insert on med_admin
for each row
begin
  select med_admin_seq.nextval into :new.MEDADMINID from dual;
end;
/
create or replace procedure PCORNetMedAdmin as
begin

PMN_DROPSQL('drop index med_admin_idx');

execute immediate 'truncate table med_admin';

insert into med_admin(patid, encounterid, medadmin_providerid, medadmin_start_date, medadmin_start_time, medadmin_stop_date,
medadmin_stop_time, medadmin_type, medadmin_code, medadmin_dose_admin, medadmin_dose_admin_unit, medadmin_source, raw_medadmin_code,
raw_medadmin_dose_admin, raw_medadmin_dose_admin_unit)

with med_start as (
    select patient_num, encounter_num, provider_id, start_date, end_date, concept_cd, modifier_cd, instance_num
    from pcornet_cdm.observation_fact_meds
    where (modifier_cd like '%MAR%New Bag%' or (modifier_cd like '%MAR%Given%' and modifier_cd not like '%Not Given%'))
)
select med_start.patient_num, med_start.encounter_num, med_start.provider_id, med_start.start_date, to_char(med_start.start_date, 'HH24:MI'), med_start.end_date, to_char(med_start.end_date, 'HH24:MI'),
'RX', med_start.concept_cd, med_dose.nval_num, med_dose.units_cd, 'OD', med_start.concept_cd, med_dose.nval_num, med_dose.units_cd
from med_start
left join pcornet_cdm.observation_fact_meds med_dose
on med_dose.instance_num = med_start.instance_num
and med_dose.modifier_cd like '%Dose%'
;

execute immediate 'create index med_admin_idx on med_admin (PATID, ENCOUNTERID)';
--GATHER_TABLE_STATS('MED_ADMIN');

end PCORNetMedAdmin;
/

SELECT 1 FROM HARVEST