
  create view "postgres"."dbt_schema"."ins_to_pit_card__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm,
       card_rk, valid_from_dttm, valid_to_dttm,
       profile_card_post_vf_dttm
    from (
    

select * from
(select 
    'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, 
    '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, 
    t1.CARD_rk, 
    '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm,
    '5999-01-01 00:00:00'::timestamp valid_to_dttm,
    
        --подставляем новую дату актуальности, в случаях, если ее нет, - последнюю дату актуальности
        coalesce(max(t2.valid_from_dttm), max(pit.profile_card_post_vf_dttm)) profile_card_post_vf_dttm 
     
from dbt_schema."GPR_BV_A_CARD" t1 
left join 
	-- Ищем записи из новой выгрузки
    
    (
        select card_rk, case when delete_flg = 1 then '1960-01-01' ::timestamp else valid_from_dttm end valid_from_dttm 
        from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
        where 
        
            actual_flg = 1  
	    and valid_from_dttm = 
	    (select max(valid_from_dttm) from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST")
            and (card_rk, valid_from_dttm) not in (select card_rk,  valid_from_dttm from dbt_schema."GPR_BV_P_CARD")
    ) t2 on t1.x_card_rk = t2.card_rk
     
    
--join pit для того, чтобы узнать последнюю дату изменения, тех экземпляров, которые не изменились в последней выгрузке 
join  (select * from dbt_schema."GPR_BV_P_CARD" where valid_to_dttm = '5999-01-01' :: timestamp) pit
on pit.card_rk = t1.card_rk
--исключение экземпляров, в которых ничего не изменилось
where ( t2.card_rk is not null )
group by t1.card_rk)
--исключение экземпляров, которые были удалены на всех источниках
where
( profile_card_post_vf_dttm > '1960-01-01' :: timestamp  )




    union all
    select * from "postgres"."dbt_schema"."ins_new_to_pit_card"
    )

--depends on "postgres"."dbt_schema"."ins_new_to_pit_card"
--depends on "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
  );