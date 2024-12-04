{{ ins_hub_macros(  '"dbt_schema"."GPR_RV_H_CARD"', 'ods_cut_client_profile_card_post_pg', "card_rk", ("card_num", )  )  }}

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}