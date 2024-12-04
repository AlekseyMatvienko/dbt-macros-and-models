

select 
    
        'scheduled__1960-01-01T00:00:00+00:00' dataflow_id,
        '2024-11-26 11:37:15.055711+00:00'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5(  id || '#' ||   oid) card_rk, 
        '2024-11-26 11:37:15.055711+00:00'::timestamp valid_from_dttm, 
        md5(  card_num || '#' ||  service_name || '#' ||  discount ) hashdiff_key,
        1 actual_flg,
        0 delete_flg,
        
            card_num
            , 
        
            service_name
            , 
        
            discount
            
        
    
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
	 "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST" where actual_flg = 1 and delete_flg = 0)
		)



--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
--depends on "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"