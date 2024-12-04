select dataflow_id, dataflow_dttm,
       hashdiff_key, card_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    {{ select_all_columns_macro('"dbt_schema"."GPR_RV_E_CARD"', 'ods_cut_client_profile_card_post_pg', ("id", ), "card_rk", (), "E") }}
    union all
    {{ select_modif_sals( '"dbt_schema"."GPR_RV_E_CARD"', 'ods_cut_client_profile_card_post_pg', ("id", ), "card_rk", ("card_num", "service_name", "discount"), "E" ) }}
    )

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}
