
  create view "postgres"."dbt_schema"."test3__dbt_tmp"
    
    
  as (
    select *, 'test3'
from "postgres"."dbt_schema"."test2"
  );