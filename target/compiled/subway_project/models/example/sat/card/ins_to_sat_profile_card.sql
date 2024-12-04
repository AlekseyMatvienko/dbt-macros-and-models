select dataflow_id, dataflow_dttm,
       source_system_dk, card_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       card_num_cnt, card_service_name_desc, discount_procent_cnt
from (
    select dataflow_id, dataflow_dttm,
        source_system_dk, card_rk, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        card_num card_num_cnt, service_name card_service_name_desc, discount discount_procent_cnt
    from "postgres"."dbt_schema"."ins_new_or_modif_sat_profile_card"
    union all
    select dataflow_id, dataflow_dttm,
        source_system_dk, card_rk, valid_from_dttm, hashdiff_key,
        actual_flg, delete_flg,
        card_num_cnt, card_service_name_desc, discount_procent_cnt
    from "postgres"."dbt_schema"."ins_del_sat_profile_card"
    )

--depends on  "postgres"."dbt_schema"."ins_new_or_modif_sat_profile_card"
--depends on "postgres"."dbt_schema"."ins_del_sat_profile_card"