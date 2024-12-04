select dataflow_id, dataflow_dttm,
       hashdiff_key, card_rk, 
       delete_flg, actual_flg, source_system_dk, valid_from_dttm
from (
    


select 
    
        'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
        '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
        hashdiff_key,
        card_rk,
        1 delete_flg,
        1 actual_flg, 
        source_system_dk,
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm
    
from 
    "dbt_schema"."GPR_RV_E_CARD"
where card_rk in
(
select 
	card_rk
from
	(
	select 
		card_rk
	from 
		"dbt_schema"."GPR_RV_E_CARD"
	 where delete_flg = 0 and actual_flg = 1 and source_system_dk = (select max(oid) from "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg")
    except
    select 
		md5(  id || '#' ||   oid) entity_rk 
	from 
		 "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg" )
		)
	and actual_flg = 1


    union all
    

select 
    
        'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
        '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
        md5(  card_num || '#' ||  service_name || '#' ||  discount ) hashdiff_key,
        md5(  id || '#' ||   oid) card_rk,
        0 delete_flg,
        1 actual_flg, 
        oid source_system_dk,
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm
    
from 
	"postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
where md5(  id || '#' ||   oid) in
(
select 
	card_rk
from
	(
	select 
		md5(  id || '#' ||   oid) card_rk, 
		md5(  card_num || '#' || service_name || '#' || discount ) hashdiff_key 
	from 
	    "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
	except
	select 
		card_rk, 
		hashdiff_key   
	from 
	 "dbt_schema"."GPR_RV_E_CARD" where actual_flg = 1 and delete_flg = 0)
		)


    )

--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"