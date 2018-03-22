/** dispensing - create and populate the dispensing table.
*/
select encounterid from encounter where 'dep' = 'encounter.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE dispensing');
END;
/
CREATE TABLE dispensing(
	DISPENSINGID varchar(19)  primary key,
	PATID number(38, 0) NOT NULL,
	PRESCRIBINGID varchar(19)  NULL,
	DISPENSE_DATE date NOT NULL,
	NDC varchar (11) NOT NULL,
	DISPENSE_SUP number(18) NULL,
	DISPENSE_AMT number(18) NULL,
	RAW_NDC varchar (50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  dispensing_seq');
END;
/
create sequence  dispensing_seq
/

create or replace trigger dispensing_trg
before insert on dispensing
for each row
begin
  select dispensing_seq.nextval into :new.DISPENSINGID from dual;
end;
/

BEGIN
PMN_DROPSQL('DROP TABLE DISP_SUPPLY');  --Changed the table 'supply' to the name 'disp_supply' to avoid conflicts with the prescribing procedure, Matthew Joss 8/16/16
END;
/

CREATE TABLE DISP_SUPPLY  (
	NVAL_NUM     	NUMBER(18,5) NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL
	)
/

BEGIN
PMN_DROPSQL('DROP TABLE amount');
END;
/

CREATE TABLE AMOUNT  (
	NVAL_NUM     	NUMBER(18,5) NULL,
	ENCOUNTER_NUM	NUMBER(38) NOT NULL,
	CONCEPT_CD   	VARCHAR2(50) NOT NULL
	)
/
BEGIN
PMN_DROPSQL('drop index dispensing_idx');
END;
/
create or replace procedure PCORNetDispensing(patient_num_first int, patient_num_last int) as
begin

/*
PMN_DROPSQL('DROP TABLE supply');
sqltext := 'create table supply as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact supply '||
'        inner join encounter enc on enc.patid = supply.patient_num and enc.encounterid = supply.encounter_Num '||
'      join pcornet_med supplycode  '||
'        on supply.modifier_cd = supplycode.c_basecode '||
'        and supplycode.c_fullname like ''\PCORI_MOD\RX_DAYS_SUPPLY\'' ) ';
PMN_EXECUATESQL(sqltext);


PMN_DROPSQL('DROP TABLE amount');
sqltext := 'create table amount as '||
'(select nval_num,encounter_num,concept_cd from i2b2fact amount '||
'     join pcornet_med amountcode '||
'        on amount.modifier_cd = amountcode.c_basecode '||
'        and amountcode.c_fullname like ''\PCORI_MOD\RX_QUANTITY\'') ';
PMN_EXECUATESQL(sqltext);
*/

/* NOTE: New transformation developed by KUMC */

insert /*+ APPEND*/ into dispensing (
	PATID
  ,PRESCRIBINGID
  ,DISPENSE_DATE -- using start_date from i2b2
  ,NDC --using pcornet_med pcori_ndc - new column!
  ,DISPENSE_SUP ---- modifier nval_num
  ,DISPENSE_AMT  -- modifier nval_num
--    ,RAW_NDC
)
/* Below is the Cycle 2 fix for populating the DISPENSING table  */
with pnm as (
  select  c_basecode, c_fullname
  from "BLUEHERONMETADATA".pcornet_med pnm
)
, basis_meta as (
  select c_basecode
  from pnm
  where pnm.c_fullname between '\PCORI_MOD\RX_BASIS\DI\' and '\PCORI_MOD\RX_BASIS\DI\~'
)
, qty_meta as (
  select c_basecode
  from pnm
  where pnm.c_fullname between '\PCORI_MOD\RX_QUANTITY\' and '\PCORI_MOD\RX_QUANTITY\~'
)
, supply_meta as (
  select c_basecode
  from pnm
  where pnm.c_fullname between '\PCORI_MOD\RX_DAYS_SUPPLY\' and '\PCORI_MOD\RX_DAYS_SUPPLY\~'
)

, ndc_pat_grp as (
  select /*+ parallel(0) */ ibf.*, replace(concept_cd, 'NDC:', '') ndc
  from i2b2fact ibf
  where patient_num between 1 and 13000 --  :patient_num_first and :patient_num_last
  and concept_cd between 'NDC:' and 'NDC:~'
  and length(concept_cd) <= (4 + 11)
)

, disp_status as
  (select ibf.patient_num
  , ibf.encounter_num
  , ibf.concept_cd
  , ibf.instance_num
  , ibf.start_date
  , ibf.modifier_cd
  , ibf.ndc
  from ndc_pat_grp ibf
  where exists (select 1 from basis_meta where ibf.modifier_cd = basis_meta.c_basecode))

, disp_quantity as
  (select ibf.patient_num
  , ibf.encounter_num
  , ibf.concept_cd
  , ibf.instance_num
  , ibf.start_date
  , ibf.modifier_cd
  , ibf.nval_num
  from ndc_pat_grp ibf
  where exists (select 1 from qty_meta where ibf.modifier_cd = qty_meta.c_basecode))

, disp_supply as
  (select ibf.patient_num
  , ibf.encounter_num
  , ibf.concept_cd
  , ibf.instance_num
  , ibf.start_date
  , ibf.modifier_cd
  , ibf.nval_num
  from ndc_pat_grp ibf
  where exists (select 1 from supply_meta where ibf.modifier_cd = supply_meta.c_basecode))


select distinct st.patient_num patid
, null prescribingid
, st.start_date dispense_date
, st.ndc
, ds.nval_num dispense_sup
, qt.nval_num dispense_amt
from disp_status st
left outer join disp_quantity qt
on st.patient_num      = qt.patient_num
  and st.encounter_num = qt.encounter_num
  and st.concept_cd    = qt.concept_cd
  and st.instance_num  = qt.instance_num
  and st.start_date    = qt.start_date
left outer join disp_supply ds
on st.patient_num      = ds.patient_num
  and st.encounter_num = ds.encounter_num
  and st.concept_cd    = ds.concept_cd
  and st.instance_num  = ds.instance_num
  and st.start_date    = ds.start_date
;

commit;

/* NOTE: The original SCILHS transformation is below.

-- insert data with outer joins to ensure all records are included even if some data elements are missing

select  m.patient_num, null,m.start_date, NVL(mo.pcori_ndc,'NA')
    ,max(supply.nval_num) sup, max(amount.nval_num) amt
from i2b2fact m inner join pcornet_med mo
on m.concept_cd = mo.c_basecode
inner join encounter enc on enc.encounterid = m.encounter_Num

    -- jgk bugfix 11/2 - we weren't filtering dispensing events
    inner join (select pcori_basecode,c_fullname,encounter_num,concept_cd from i2b2fact basis
        inner join encounter enc on enc.patid = basis.patient_num and enc.encounterid = basis.encounter_Num
     join pcornet_med basiscode
        on basis.modifier_cd = basiscode.c_basecode
        and basiscode.c_fullname='\PCORI_MOD\RX_BASIS\DI\') basis
    on m.encounter_num = basis.encounter_num
    and m.concept_cd = basis.concept_Cd

    left join  supply
    on m.encounter_num = supply.encounter_num
    and m.concept_cd = supply.concept_Cd


    left join  amount
    on m.encounter_num = amount.encounter_num
    and m.concept_cd = amount.concept_Cd

group by m.encounter_num ,m.patient_num, m.start_date,  mo.pcori_ndc;
*/

end PCORNetDispensing;
/
insert into cdm_status (status, last_update) select 'dispensing_initialize', sysdate from dual
/
select 1 from cdm_status where status = 'dispensing_initialize'