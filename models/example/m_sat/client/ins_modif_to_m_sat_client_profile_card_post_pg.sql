{{ select_modif_m_sat(
    '"dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"',
    'ods_cut_client_profile_card_post_pg',
    ("id", ),
    "client_rk",
    ("birthday", "fio"),
    "row_num",
    ("fio", "phone_num", "birthday"),
    ("fio_desc", "phone_desc", "birthday_dt", "valid_from_dttm")
) }}

--depends on {{ ref('ods_cut_client_profile_card_post_pg') }}