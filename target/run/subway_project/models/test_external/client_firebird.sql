
  
    

  create  table "postgres"."dbt_schema"."client_firebird__dbt_tmp"
  
  
    as
  
  (
    select
    id as pk
    , name as client_name
from "SUBWAY"."firebird"."client"
  );
  