select dataflow_id, dataflow_dttm,
       source_system_dk, client_rk, row_num, valid_from_dttm, hashdiff_key,
       actual_flg, delete_flg,
       fio_desc, phone_desc, birthday_dt
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
	row_num, 
        '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        
            fio_desc
            , 
        
            phone_desc
            , 
        
            birthday_dt
            
        
    
from 
    "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
where (
    
        client_rk
        , 
        
            fio_desc
            , 
        
            phone_desc
            , 
        
            birthday_dt
            
        
    
    ) in
(
select 
    
        client_rk
        , 
        
            fio_desc
            , 
        
            phone_desc
            , 
        
            birthday_dt
            
        
    
from
	(
	select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            client_rk
            , 
            
                fio_desc
                , 
            
                phone_desc
                , 
            
                birthday_dt
                
            
        
	from 
		 "dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
	 where delete_flg = 0 and actual_flg = 1
    except
    select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            md5(  id || '#' ||   oid) entity_rk
            , 
            
                fio
                , 
            
                phone_num
                , 
            
                birthday 
                
            
        
	from 
		 "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg" )
		)
	and actual_flg = 1
    and delete_flg = 0


    union all
    select * from "postgres"."dbt_schema"."ins_modif_to_m_sat_client_profile_card_post_pg"   )

--depends on "postgres"."dbt_schema"."ods_cut_client_profile_card_post_pg"
--depends on "postgres"."dbt_schema"."ins_modif_to_m_sat_client_profile_card_post_pg"