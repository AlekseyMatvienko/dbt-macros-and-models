with sys_code as (select max(oid) soid from {{ source('dbt_schema', 'ods_client_csv') }})

select '{{ var('run_id') }}' run_id, '{{ var('execution_date') }}'::timestamp execution_date, 
	    md5(id_operation || '#' || id_seller || '#' || client_rk || '#' || id_product || '#' || id_product_connection || '#' || sel_dttm || '#' || tovar_group || '#' || oid) receip_rk, -- Подставили атрибуты вместо ключей, как заглушка, пока нет измерений
	    md5(id_seller || '#' || oid) shop_rk, -- Заглушка, пока нет сущности в проекте
		client_rk, 
		md5(id_product || '#' || oid) plu_rk, -- Заглушка, пока нет сущности в проекте
	    md5(id_product_connection || '#' || oid) plu_x_plu_rk, -- Заглушка, пока нет сущности в проекте
		id_disc_card card_cnt, 
		id_operation receip_num_cnt,
	    sel_dttm, cnt, price, combo_group, tovar_group
from {{ref('ods_cut_receipt_profile_card_post_pg')}} tt
join  {{ source('dbt_schema', "GPR_RV_H_CLIENT") }} hc on regexp_instr(hc.hub_key, tt.id_buyer || '#') = 1
cross join sys_code sc 
where hc.hub_key like '%#' || soid and id_operation not in (select receip_num_cnt from dbt_schema."GPR_RV_T_RECEIPT_POST" )

--depends on {{ref('ods_cut_receipt_profile_card_post_pg')}}
--depends on {{ source('dbt_schema', 'ods_client_csv') }}
--depends on {{ source('dbt_schema', 'GPR_RV_H_CLIENT') }}