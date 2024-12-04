select dataflow_id, dataflow_dttm,
       hashdiff_key, client_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    {{ select_modif_sals('"dbt_schema"."GPR_RV_E_CLIENT"', 'ods_cut_client_from_star_orcl', ("id", ), "client_rk", ("name", "phone", "city", "birthday", "age"), "E" ) }}
    union all
    {{ select_all_columns_macro('"dbt_schema"."GPR_RV_E_CLIENT"', 'ods_cut_client_from_star_orcl', ("id", ), "client_rk", ( ), "E") }}
    )

--depends on {{ref('ods_cut_client_from_star_orcl')}}
