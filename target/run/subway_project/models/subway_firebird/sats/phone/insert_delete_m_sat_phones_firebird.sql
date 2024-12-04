
  create view "postgres"."dbt_schema"."insert_delete_m_sat_phones_firebird__dbt_tmp"
    
    
  as (
     

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
        
            phone_number_desc
            , 
        
            question1_cnt
            , 
        
            question2_cnt
            , 
        
            question3_cnt
            
        
    
from 
    "dbt_schema"."GPR_RV_M_CLIENT_PHONES"
where (
    
        client_rk
        , 
        
            phone_number_desc
            , 
        
            question1_cnt
            , 
        
            question2_cnt
            , 
        
            question3_cnt
            
        
    
    ) in
(
select 
    
        client_rk
        , 
        
            phone_number_desc
            , 
        
            question1_cnt
            , 
        
            question2_cnt
            , 
        
            question3_cnt
            
        
    
from
	(
	select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            client_rk
            , 
            
                phone_number_desc
                , 
            
                question1_cnt
                , 
            
                question2_cnt
                , 
            
                question3_cnt
                
            
        
	from 
		 "dbt_schema"."GPR_RV_M_CLIENT_PHONES"
	 where delete_flg = 0 and actual_flg = 1
    except
    select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        
            md5(  id_client || '#' ||   oid) entity_rk
            , 
            
                phone_number
                , 
            
                question1
                , 
            
                question2
                , 
            
                question3
                
            
        
	from 
		 "postgres"."dbt_schema"."firebird_cut_phone_ods" )
		)
	and actual_flg = 1
    and delete_flg = 0


  );