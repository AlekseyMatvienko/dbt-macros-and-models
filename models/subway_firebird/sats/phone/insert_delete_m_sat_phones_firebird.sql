{{ select_all_columns_macro_new(
    '"dbt_schema"."GPR_RV_M_CLIENT_PHONES"', 
    'firebird_cut_phone_ods', 
    ('id_client', ),
    'client_rk', 
    'row_num', 
    ('phone_number_desc', 'question1_cnt', 'question2_cnt', 'question3_cnt'), 
    ('phone_number_desc', 'question1_cnt', 'question2_cnt', 'question3_cnt'), 
    ('phone_number', 'question1', 'question2', 'question3'), 
    'M'
) 
}}