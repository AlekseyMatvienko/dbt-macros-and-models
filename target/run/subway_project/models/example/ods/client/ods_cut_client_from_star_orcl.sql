
  
    

  create  table "postgres"."dbt_schema"."ods_cut_client_from_star_orcl__dbt_tmp"
  
  
    as
  
  (
    

select * from "postgres"."dbt_schema"."ods_client_csv"  where execution_date = '2024-12-03 11:00:52.076204+00:00'

--depends on "postgres"."dbt_schema"."ods_client_csv"
  );
  