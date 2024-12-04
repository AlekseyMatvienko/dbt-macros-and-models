
  
    

  create  table "postgres"."dbt_schema"."phone_firebird__dbt_tmp"
  
  
    as
  
  (
    select
    id as pk
    , phone_number as phone_client
    , question1 as question1
    , question2 as question2
    , question3 as question3
    , id_client as id_client
from "SUBWAY"."firebird"."phone"
  );
  