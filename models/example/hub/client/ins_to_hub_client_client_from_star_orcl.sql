{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CLIENT"',  'ods_cut_client_from_star_orcl' , "client_rk", ("id", )  )  }}

--depends on {{ ref('ods_cut_client_from_star_orcl') }}