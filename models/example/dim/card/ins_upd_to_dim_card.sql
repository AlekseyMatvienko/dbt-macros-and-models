{{ ins_dim_macros
(
	source("dbt_schema", "GPR_BV_A_CARD"),
	(
		source("dbt_schema", "GPR_BV_P_CARD"),
		(
			"profile_card_post_vf_dttm", 
		)
	),
	"card_rk",
	(
		(
			source("dbt_schema", "GPR_RV_S_PROFILE_CARD_POST"), 
			"GPR_RV_S_PROFILE_CARD_POST", 
			("card_num_cnt", "card_service_name_desc", "discount_procent_cnt"),
			"S",
			"107023"
		),
	),
	(
		"card_num_cnt",
		"card_service_name_desc",
		"discount_procent_cnt"
	)
) 
}}


--depends on {{ source('dbt_schema', 'GPR_RV_S_PROFILE_CARD_POST') }}
--depends on {{ source('dbt_schema', 'GPR_BV_A_CARD') }}
--depends on {{ source('dbt_schema', 'GPR_BV_P_CARD') }}
