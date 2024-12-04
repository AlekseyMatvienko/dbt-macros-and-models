
  
    

  create  table "postgres"."dbt_schema"."ods_cut_receipt_profile_card_post_pg__dbt_tmp"
  
  
    as
  
  (
    

select * from ods_receipt_post where execution_date = '2024-11-22 11:45:23.346621+00:00'
  );
  