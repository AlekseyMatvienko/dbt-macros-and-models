{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CLIENT"', 'ods_cut_client_profile_card_post_pg', "client_rk", ("id", )  )  }}

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}