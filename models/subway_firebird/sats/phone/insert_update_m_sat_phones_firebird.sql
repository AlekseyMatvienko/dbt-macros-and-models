{{ select_modif_m_sat(
    'dbt_schema."GPR_RV_M_CLIENT_PHONES"',
    'firebird_cut_phone_ods',
    ('id_client', ),
    'client_rk',
    ('id_client', ),
    'row_num',
    ('phone_number', 'question1', 'question2', 'question3'),
    ('phone_number_desc', 'question1_cnt', 'question2_cnt', 'question3_cnt')
) }}