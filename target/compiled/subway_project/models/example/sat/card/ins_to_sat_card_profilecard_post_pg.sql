select dataflow_id, dataflow_dttm,
       source_system_dk, card_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       card_num_cnt, card_service_name_desc, discount_procent_cnt
    from (
     

/*
Поля для случая м сата:
    attrs_target=() - tuple строк - названия столбцов таблицы м сата - бизнес атрибутов
    attrs_source=() - tuple строк - названия столбцов таблицы источника - бизнес атрибутов

они нужны для того, чтобы правильно "вычитать" (except) выборки, 
и находить удаленные записи
*/

-- макрос, который смотрит какие строки удалены
select
    
        'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
        '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
        source_system_dk, 
        card_rk,
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            card_num_cnt
            , 
        
            card_service_name_desc
            , 
        
            discount_procent_cnt
            
        
    
from 
    "dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
where (
    
        card_rk
    
    ) in
(
select 
    
        card_rk
    
from
	(
	select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            card_rk
        
	from 
		 "dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
	 where delete_flg = 0 and actual_flg = 1
    except
    select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            md5(  card_num_cnt || '#' ||   oid) entity_rk
        
	from 
		 "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg" )
		)
	and actual_flg = 1
    and delete_flg = 0


    union all
     

select 
    
        'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
        '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5(  card_num || '#' ||   oid) card_rk, 
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm, 
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
where md5(  card_num || '#' ||   oid) in
(
select 
	card_rk
from
	(
	select 
		md5(  card_num || '#' ||   oid) card_rk, 
		md5(  card_num || '#' || service_name || '#' || discount ) hashdiff_key 
	from 
	    "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
	except
	select 
		card_rk, 
		hashdiff_key   
	from 
	 "dbt_schema"."GPR_RV_S_PROFILE_CARD_POST" where actual_flg = 1 and delete_flg = 0)
		)


    )
    

--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"