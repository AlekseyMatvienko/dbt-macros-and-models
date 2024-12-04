
  create view "postgres"."dbt_schema"."firebird_cut_client_ods__dbt_tmp"
    
    
  as (
    select * from dbt_schema.ods_firebird_client where dttm = '2024-12-03 11:00:52.076204+00:00'
  );