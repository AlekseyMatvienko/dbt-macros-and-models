select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       fio_desc, phone_desc, birthday_dt
    from (
    {{ select_all_columns_macro_new('"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"', 'ods_cut_client_profile_card_post_pg', ("id", ), "client_rk" , "row_num",
("fio_desc", "phone_desc", "birthday_dt"), ("fio_desc", "phone_desc", "birthday_dt"), ("fio", "phone_num", "birthday "), "M") }}
    union all
    select * from {{ref('ins_modif_to_m_sat_client_profile_card_post_pg')}}   )

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}
--depends on {{ ref('ins_modif_to_m_sat_client_profile_card_post_pg') }}