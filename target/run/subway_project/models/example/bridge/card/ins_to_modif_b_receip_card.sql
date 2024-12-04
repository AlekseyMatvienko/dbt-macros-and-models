
  create view "postgres"."dbt_schema"."ins_to_modif_b_receip_card__dbt_tmp"
    
    
  as (
    with cut_bridge as (

select 
	*
from 
	dbt_schema."GPR_BV_B_RECEIPT_CARD_POST"
where 
	(card_tech_key, valid_from_dttm)
	in 
	(select 
		card_tech_key, 
		max(valid_from_dttm) mx_valid 
	from 
		dbt_schema."GPR_BV_B_RECEIPT_CARD_POST" 
	group by card_tech_key )
)
 ,

 cut_transaction as (
 
 		select 
			client_rk,
			md5(card_cnt || '#' || '3055104511' ) card_hash ,
			card_cnt ,
			max(dataflow_dttm) dataflow_dttm 
		from 
			dbt_schema."GPR_RV_T_RECEIPT_POST" group by client_rk, card_cnt
	
 )	
	
 
select
	'manual__2024-11-22T11:45:23.346621+00:00' dataflow_id , 
    '2024-11-22 11:45:23.346621+00:00'::timestamp dataflow_dttm, 
	used_card.card_hash card_tech_key,
	'2024-11-22 11:45:23.346621+00:00'::timestamp valid_from_dttm , 
	s_cl.name_desc client_name_desc,
	s_cl.phone_desc client_phone_desc,
	s_cl.city_desc client_city_desc,
	s_cl.birthday_dt client_birthday_dt,
	s_cl.age_cnt client_age_cnt,
	used_card.card_cnt client_card_cnt

FROM 
	(cut_transaction ct
JOIN 
	cut_bridge cd
ON ct.card_hash = cd.card_tech_key AND ct.dataflow_dttm > cd.valid_from_dttm  
  
    )  used_card
JOIN 
	(
	SELECT 
		not_unif_client_rk,
		name_desc,
		CASE 
			WHEN phone_cnt = 1 THEN phone_desc
			ELSE NULL
		END phone_desc,
		city_desc,
		birthday_dt,
		age_cnt
	FROM 
		(
		SELECT
			a_cl.client_rk ,
			m_cl.client_rk not_unif_client_rk,
			m_cl.name_desc,
			m_cl.phone_desc,
			m_cl.city_desc,
			m_cl.birthday_dt,
			m_cl.age_cnt,
			COUNT(phone_desc) OVER(PARTITION BY a_cl.client_rk) phone_cnt  
		FROM 
			dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR" m_cl 
		JOIN 
			dbt_schema."GPR_BV_A_CLIENT" a_cl 
		ON m_cl.client_rk = a_cl.x_client_rk 
		WHERE actual_flg = 1 AND delete_flg = 0
		)
	) s_cl
ON s_cl.not_unif_client_rk = used_card.client_rk 
AND ( used_card.client_name_desc != s_cl.name_desc OR 
	  (used_card.client_phone_desc != s_cl.phone_desc AND (used_card.client_phone_desc IS NOT NULL AND s_cl.phone_desc IS NOT NULL )) OR
	  used_card.client_city_desc != s_cl.city_desc OR
	  used_card.client_birthday_dt != s_cl.birthday_dt OR
	  used_card.client_age_cnt != s_cl.age_cnt)
  );