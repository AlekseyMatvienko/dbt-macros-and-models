{{ ins_dim_macros
(
	source("dbt_schema", "GPR_BV_A_CLIENT"),
	(
		source("dbt_schema", "GPR_BV_P_CLIENT"),
		(
			"client_subway_star_vf_dttm", 
			"profile_client_post_vf_dttm"
		)
	),
	"client_rk",
	(
		(
			source("dbt_schema", "GPR_RV_M_CLIENT_SUBWAY_STAR"), 
			"GPR_RV_M_CLIENT_SUBWAY_STAR", 
			("name_desc", "phone_desc", "city_desc", "birthday_dt"),
			"M",
			"3515641477"
		),
		(
			source("dbt_schema", "GPR_RV_M_CLIENT_PROFILE_POST"), 
			"GPR_RV_M_CLIENT_PROFILE_POST", 
			("fio_desc", "phone_desc", "null", "birthday_dt"),
			"M",
			"107023"
		),
		(
			source("dbt_schema", "GPR_RV_S_CLIENT_CLIENT_FIREBIRD"), 
			"GPR_RV_S_CLIENT_CLIENT_FIREBIRD", 
			("name_desc", "null", "null", "birthdate_dt"),
			"",
			"1"
		)
	),
	(
		"client_name_desc",
		"client_phone_desc",
		"client_city_desc",
		"client_birthday_dt"
	)
) 
}}


--depends on {{ source('dbt_schema', 'GPR_RV_M_CLIENT_SUBWAY_STAR') }}
--depends on {{ source('dbt_schema', 'GPR_RV_M_CLIENT_PROFILE_POST') }}
--depends on {{ source('dbt_schema', 'GPR_RV_S_CLIENT_CLIENT_FIREBIRD') }}
--depends on {{ source('dbt_schema', 'GPR_BV_A_CLIENT') }}
--depends on {{ source('dbt_schema', 'GPR_BV_P_CLIENT') }}
