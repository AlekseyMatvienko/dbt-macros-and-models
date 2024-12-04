select dataflow_id, dataflow_dttm,
       source_system_dk, card_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       card_num_cnt, card_service_name_desc, discount_procent_cnt
    from (
    {{ select_all_columns_macro_new(
        '"dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"', 
        'ods_cut_client_profile_card_post_pg', 
        ("card_num_cnt", ), 
        "card_rk" , 
        null,
        ("card_num_cnt", "card_service_name_desc", "discount_procent_cnt")
    ) }}
    union all
     {{ select_modif_sals(
    	'"dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"',
    	'ods_cut_client_profile_card_post_pg',
    	("card_num", ),
    	"card_rk",
    	("card_num", "service_name", "discount")
      ) }}
    )
    

--depends on {{ref('ods_cut_client_profile_card_post_pg')}}