{{ select_modif_m_sat(
        '"dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"', 
        'ods_cut_client_from_star_orcl', 
        ("id", ), 
        "client_rk", 
        ("birthday", "name"), 
        "row_num",  
        ("name", "phone", "city", "birthday", "age"),
        ("name_desc", "phone_desc", "city_desc", "birthday_dt", "age_cnt")
    ) }}

--depends on {{ref('ods_cut_client_from_star_orcl')}}