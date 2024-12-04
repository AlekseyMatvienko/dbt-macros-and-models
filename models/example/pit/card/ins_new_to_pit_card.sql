{{ ins_pit_new_macros('CARD', ((source("dbt_schema", "GPR_RV_S_PROFILE_CARD_POST"), 'profile_card_post_vf_dttm', ''), ), 'card_rk') }}

--depends on {{ source('dbt_schema', 'GPR_RV_S_PROFILE_CARD_POST') }}
