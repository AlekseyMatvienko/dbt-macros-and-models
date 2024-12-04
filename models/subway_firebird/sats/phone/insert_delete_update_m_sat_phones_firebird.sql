select 
    dataflow_id
    , dataflow_dttm
    , source_system_dk
    , client_rk
    , valid_from_dttm
    , row_num
    , hashdiff_key
    , actual_flg
    , delete_flg
    , phone_number_desc
    , question1_cnt
    , question2_cnt
    , question3_cnt
from {{ ref('insert_delete_m_sat_phones_firebird') }}
union all
select 
	dataflow_id
    , dataflow_dttm
    , source_system_dk
    , client_rk
    , valid_from_dttm
    , row_num
    , hashdiff_key
    , actual_flg
    , delete_flg
    , phone_number
    , question1
    , question2
    , question3
 from {{ ref('insert_update_m_sat_phones_firebird') }}

--depends on {{ref('insert_delete_m_sat_phones_firebird')}}
--depends on {{ref('insert_update_m_sat_phones_firebird')}}