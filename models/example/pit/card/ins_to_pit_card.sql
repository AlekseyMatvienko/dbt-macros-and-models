select dataflow_id, dataflow_dttm,
       card_rk, valid_from_dttm, valid_to_dttm,
       profile_card_post_vf_dttm
    from (
    {{ ins_pit_modif_macros('CARD', 
                       ((source("dbt_schema", "GPR_RV_S_PROFILE_CARD_POST"), 'profile_card_post_vf_dttm', ""), ), 
                       'card_rk') }}
    union all
    select * from {{ ref('ins_new_to_pit_card') }}
    )

--depends on {{ ref('ins_new_to_pit_card') }}
--depends on {{ source('dbt_schema', 'GPR_RV_S_PROFILE_CARD_POST') }}