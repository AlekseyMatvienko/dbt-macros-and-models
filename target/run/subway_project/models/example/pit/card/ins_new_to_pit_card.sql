
  create view "postgres"."dbt_schema"."ins_new_to_pit_card__dbt_tmp"
    
    
  as (
    

with pit_new as (
	-- Узнаем унифицированные rk и проверяем, есть ли запись с таким rk в pit таблице
    select 'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, t1.CARD_rk, 
    '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm,
    '5999-01-01 00:00:00'::timestamp valid_to_dttm,
    
        coalesce(max(t2.valid_from_dttm), '1960-01-01 00:00:00'::timestamp) profile_card_post_vf_dttm 
     
    from dbt_schema."GPR_BV_A_CARD" t1 
    left join 
    	-- Выбор из последней выгрузки актуальных неудаленных записей
        
        (
            select card_rk, valid_from_dttm, source_system_dk 
            from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
            where 
            
                actual_flg = 1 
                and delete_flg = 0 
                and valid_from_dttm  = 
                (select max(valid_from_dttm) from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST") 
        ) t2 on t1.x_card_rk = t2.card_rk
         
        
    where t1.card_rk not in (select card_rk from dbt_schema."GPR_BV_P_CARD")
    group by t1.card_rk
)

--Добавляем каждому rk ополнительный период от 1960 года до даты выгрузки
select * from pit_new
union all
select 'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, card_rk, 
       '1960-01-01 00:00:00'::timestamp valid_from_dttm, valid_from_dttm - interval '1 minute' valid_to_dttm, 
        
            '1960-01-01 00:00:00'::timestamp profile_card_post_vf_dttm 
         
from pit_new



--depends on "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
  );