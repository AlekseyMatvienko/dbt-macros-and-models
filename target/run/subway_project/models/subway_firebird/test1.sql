
  create view "postgres"."dbt_schema"."test1__dbt_tmp"
    
    
  as (
    select 1 as id
union all
select 2 as id
  );