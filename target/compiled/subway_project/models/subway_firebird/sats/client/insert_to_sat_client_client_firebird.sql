select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       name_desc, birthdate_dt
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
        client_rk,
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            name_desc
            , 
        
            birthdate_dt
            
        
    
from 
    "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
where (
    
        client_rk
    
    ) in
(
select 
    
        client_rk
    
from
	(
	select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            client_rk
        
	from 
		 "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
	 where delete_flg = 0 and actual_flg = 1
    except
    select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            md5(  id || '#' ||   oid) entity_rk
        
	from 
		 "postgres"."dbt_schema"."firebird_cut_client_ods" )
		)
	and actual_flg = 1
    and delete_flg = 0


    union all
     

select 
    
        'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
        '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5(  id || '#' ||   oid) client_rk, 
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm, 
        md5(  name || '#' ||  birthday ) hashdiff_key,
        1 actual_flg,
        0 delete_flg,
        
            name
            , 
        
            birthday
            
        
    
from 
	"postgres"."dbt_schema"."firebird_cut_client_ods"
where md5(  id || '#' ||   oid) in
(
select 
	client_rk
from
	(
	select 
		md5(  id || '#' ||   oid) client_rk, 
		md5(  name || '#' || birthday ) hashdiff_key 
	from 
	    "postgres"."dbt_schema"."firebird_cut_client_ods"
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
	 "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD" where actual_flg = 1 and delete_flg = 0)
		)


    )
    

--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
--depends on "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"