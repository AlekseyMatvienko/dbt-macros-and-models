select 
    t1.id as id1
    , t2.id as id2 
from (
    select 3 as id
    union all
    select 4 as id
) as t1, "postgres"."dbt_schema"."test1" as t2