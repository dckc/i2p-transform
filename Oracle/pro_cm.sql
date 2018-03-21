/** pro_cm - create the pro_cm table.
*/
select synonym_name from all_synonyms where 'dep' = 'pcornet_init.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE pro_cm');
END;
/
CREATE TABLE pro_cm(
	PRO_CM_ID varchar(19)  primary key,
	PATID number(38, 0) NOT NULL,
	ENCOUNTERID  number(38, 0) NULL,
	PRO_ITEM varchar (20) NOT NULL,
	PRO_LOINC varchar (10) NULL,
	PRO_DATE date NOT NULL,
	PRO_TIME varchar (5) NULL,
	PRO_RESPONSE int NOT NULL,
	PRO_METHOD varchar (2) NULL,
	PRO_MODE varchar (2) NULL,
	PRO_CAT varchar (2) NULL,
	RAW_PRO_CODE varchar (50) NULL,
	RAW_PRO_RESPONSE varchar (50) NULL
)
/

BEGIN
PMN_DROPSQL('DROP sequence  pro_cm_seq');
END;
/
create sequence  pro_cm_seq
/

create or replace trigger pro_cm_trg
before insert on pro_cm
for each row
begin
  select pro_cm_seq.nextval into :new.PRO_CM_ID from dual;
end;
/
insert into cdm_status (status, last_update, records) select 'pro_cm', sysdate, count(*) from pro_cm
/
select 1 from cdm_status where status = 'pro_cm'
