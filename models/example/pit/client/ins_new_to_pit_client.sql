{{ ins_pit_new_macros(
'CLIENT', 
(
	(source("dbt_schema", "GPR_RV_M_CLIENT_SUBWAY_STAR"), 'client_subway_star_vf_dttm', 'M'), 
	(source("dbt_schema", "GPR_RV_M_CLIENT_PROFILE_POST"), 'profile_client_post_vf_dttm', 'M'), 
	(source("dbt_schema", "GPR_RV_S_CLIENT_CLIENT_FIREBIRD"), 'client_phones_vf_dttm', 'S')
), 
'client_rk') }}

--depends on {{ source('dbt_schema', 'GPR_RV_M_CLIENT_SUBWAY_STAR') }}
--depends on {{ source('dbt_schema', 'GPR_RV_M_CLIENT_PROFILE_POST') }}
--depends on {{ source('dbt_schema', 'GPR_RV_S_CLIENT_CLIENT_FIREBIRD') }}