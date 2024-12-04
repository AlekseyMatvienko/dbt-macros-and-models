select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       name_desc, phone_desc, city_desc, birthday_dt, age_cnt
    from (
    {{ select_all_columns_macro_new(
        '"dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"', 
        'ods_cut_client_from_star_orcl', 
        ("id", ), 
        "client_rk" , 
        "row_num",
        ("name_desc", "phone_desc", "city_desc", "birthday_dt", "age_cnt",),
	("name_desc", "phone_desc", "city_desc", "birthday_dt", "age_cnt",),
	("name", "phone", "city", "birthday", "age",), "M"
    ) }}
    union all
    select * from {{ ref('ins_modif_to_m_sat_client_client_from_star_orcl') }}
    )

--depends on {{ref('ods_cut_client_from_star_orcl')}}
--depends on {{ ref('ins_modif_to_m_sat_client_client_from_star_orcl') }}