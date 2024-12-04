{{ union_sat_datas( ((source("dbt_schema","GPR_RV_S_PROFILE_CARD_POST"), "card_num_cnt"), ), ("card_num_cnt", ), "CARD"  )}}

--depends on {{ source("dbt_schema","GPR_RV_S_PROFILE_CARD_POST") }}