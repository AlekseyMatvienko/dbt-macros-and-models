{{ union_sat_datas( 
    (
        (source("dbt_schema","GPR_RV_M_CLIENT_PROFILE_POST"), "fio_desc", "birthday_dt" ), 
        (source("dbt_schema","GPR_RV_M_CLIENT_SUBWAY_STAR"), "name_desc", "birthday_dt"),
        (source("dbt_schema","GPR_RV_S_CLIENT_CLIENT_FIREBIRD"), "name_desc", "birthdate_dt")
    ), 
    ("name_desc", "birthday_dt"), 
    "CLIENT"  
    )
}}

--depends on {{ source("dbt_schema","GPR_RV_M_CLIENT_PROFILE_POST") }}
--depends on {{ source("dbt_schema","GPR_RV_M_CLIENT_SUBWAY_STAR") }}
--depends on {{ source("dbt_schema","GPR_RV_S_CLIENT_CLIENT_FIREBIRD") }}