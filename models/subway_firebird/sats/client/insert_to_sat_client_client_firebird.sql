select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       name_desc, birthdate_dt
    from (
    {{ select_all_columns_macro_new(
        source("dbt_schema","GPR_RV_S_CLIENT_CLIENT_FIREBIRD"), 
        'firebird_cut_client_ods', 
        ("id", ), 
        "client_rk" , 
        null,
        ("name_desc", "birthdate_dt")
    ) }}
    union all
     {{ select_modif_sals(
    	source("dbt_schema","GPR_RV_S_CLIENT_CLIENT_FIREBIRD"),
    	'firebird_cut_client_ods',
    	("id", ),
    	"client_rk",
    	("name", "birthday")
      ) }}
    )
    

--depends on {{ref('ods_cut_client_profile_card_post_pg')}}
--depends on {{source("dbt_schema","GPR_RV_S_PROFILE_CARD_POST")}}