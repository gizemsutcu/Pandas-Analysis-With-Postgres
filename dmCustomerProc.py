import sqlalchemy as sqlalchemy
import numpy as np
import datetime
import pandas as pd

# author: GİZEM SÜTÇÜ

engine = None
try:
    engine = sqlalchemy.create_engine('postgres+psycopg2://userName:password@ip:port/dbName')
    print("Opened database succesfully")
except:
    print("Database not connected")

v_date = datetime.datetime.now()

# SQL-1
df_a = pd.read_sql_table("stg_dce_party", engine, "dwh_stg")
df_b = pd.read_sql_table("stg_dce_cust", engine, "dwh_stg")
df_gst = pd.read_sql_table("stg_dce_gnl_st", engine, "dwh_stg")
df_ctp = pd.read_sql_table("stg_dce_cust_tp", engine, "dwh_stg")
df_gtp = pd.read_sql_table("stg_dce_gnl_tp", engine, "dwh_stg")
df_b.rename(columns={"st_id": "st_id_cust"}, inplace=True)
df_a.rename(columns={"st_id": "st_id_party", "sdate": "sdate_party", "edate": "edate_party", "cdate": "cdate_party",
                     "cuser": "cuser_party", "udate": "udate_party", "uuser": "uuser_party"}, inplace=True)
df_ctp.rename(columns={"name": "cust_tp"}, inplace=True)
df_gst.rename(columns={"name": "st"}, inplace=True)
df_gtp.rename(columns={"name": "party_tp"}, inplace=True)

result = pd.merge(df_b[['cust_id', 'party_id', 'st_id_cust', 'cust_tp_id', 'new_cust_id', 'cust_since']],
                  df_a[['party_id', 'party_tp_id', 'st_id_party', 'frst_name', 'mname', 'lst_name', 'nick_name',
                        'edu_id', 'brth_date', 'brth_plc', 'gendr_id', 'mrtl_st_id', 'mthr_name', 'fthr_name',
                        'scl_secr_num', 'occp_id', 'incm_lvl_id', 'nat_id', 'org_name', 'org_tp_id', 'tax_id',
                        'tax_ofc', 'sdate_party', 'edate_party', 'cdate_party', 'cuser_party', 'udate_party',
                        'uuser_party', 'email', 'mobile_phone', 'fax', 'home_phone', 'facebook_url', 'linkedin_url',
                        'wrk_phone', 'refer_code']],
                  how='left', left_on=['party_id'], right_on=['party_id'])

result1 = pd.merge(result, df_gst[['gnl_st_id', 'st']], how='inner', left_on=['st_id_cust'], right_on=['gnl_st_id'])
result2 = pd.merge(result1, df_ctp[['cust_tp_id', 'cust_tp']], how='inner',
                   left_on=['cust_tp_id'], right_on=['cust_tp_id'])

result_sql1 = pd.merge(result2, df_gtp[['gnl_tp_id', 'party_tp']], how='inner',
                       left_on=['party_tp_id'], right_on=['gnl_tp_id'])
del result_sql1['gnl_st_id'], result_sql1['gnl_tp_id']
# to save the analysis result to the database
# result_sql1.to_sql("pandas_sql1", engine, "dwh", if_exists="replace", index=False)
# result_sql1.to_sql("pandas_sql1", engine, "dwh", if_exists="append", index=False)

# SQL-2
df = pd.read_sql_table("stg_dce_cust_acct", engine, "dwh_stg")
cust_acct = df.groupby(['cust_id']).size().reset_index(name='cust_acct_count')
result_sql2 = pd.merge(result_sql1, cust_acct, how='left', left_on=['cust_id'], right_on=['cust_id'])

# SQL-3
ccca = pd.read_sql_table("stg_dce_credit_card_cust_acct", engine, "dwh_stg")
df1 = pd.merge(ccca, df, how='inner', left_on=['cust_acct_id'], right_on=['cust_acct_id'])
credit_card_count = df1.groupby(['cust_id']).size().reset_index(name='credit_card_count')
result_sql3 = pd.merge(result_sql2, credit_card_count, how='left', left_on=['cust_id'], right_on=['cust_id'])

# SQL-4
df = pd.read_sql_table("stg_dce_addr", engine, "dwh_stg")
lpm = pd.read_sql_table("stg_dce_lylty_prg_memb", engine, "dwh_stg")
lpm.rename(columns={"cust_id": "cust_id_lpm"}, inplace=True)
df["rank"] = df.groupby("row_id")["addr_id"].rank("dense", ascending=False)
addr = df[['city_id', 'city_name', 'cntry_id', 'cntry_name', 'row_id', 'rank']]
addr2 = addr.loc[addr['rank'] == 1.0]
result = pd.merge(result_sql3, addr2, how='left', left_on=['cust_id'], right_on=['row_id'])
result_sql4 = pd.merge(result, lpm['cust_id_lpm'], how='left', left_on=['cust_id'], right_on=['cust_id_lpm'])
result_sql4['is_prg_memb'] = result_sql4['cust_id_lpm'].apply(lambda x: 1 if pd.notnull(x) else 0)
del result_sql4['cust_id_lpm']

# SQL-5
cacq = pd.read_sql_table("stg_dce_cust_acq", engine, "dwh_stg")
cacq["rank"] = cacq.groupby("cust_id")["cust_acq_id"].rank("dense", ascending=False)
cust_acq = cacq[['cust_id', 'web_acq_source', 'web_acq_medium', 'web_acq_campaign', 'cdate', 'rank']]
cust_acq2 = cust_acq.loc[cust_acq['rank'] == 1.0]
cust_acq2 = cust_acq2.sort_values(by='cdate', ascending=False)
result_sql5 = pd.merge(result_sql4, cust_acq2[['cust_id', 'web_acq_source', 'web_acq_medium', 'web_acq_campaign']],
                       how='left', left_on=['cust_id'], right_on=['cust_id'])

# SQL-6
dwf = pd.read_sql_table("dwf_gift_detail", engine, "dwh")
d = dwf.loc[(dwf['trgt_cust_id'] != ' ')]
gd = d[['src_cust_id']].drop_duplicates()
result_sql6 = pd.merge(result_sql5, gd, how='left', left_on=['cust_id'], right_on=['src_cust_id'])
result_sql6['is_gift'] = result_sql6['src_cust_id'].apply(lambda x: 1 if pd.notnull(x) else 0)
del result_sql6['src_cust_id']

# SQL-7
stg = pd.read_sql_table("stg_dce_refer_invit_hstr", engine, "dwh_stg")
d = stg.loc[(stg['st_id'] == 10751) & (str(stg['src_alt_val']) != stg['trgt_alt_val'])]
referral_detail = d[['src_cust_id']].drop_duplicates()
result_sql7 = pd.merge(result_sql6, referral_detail, how='left', left_on=['cust_id'], right_on=['src_cust_id'])
result_sql7['is_referral'] = result_sql7['src_cust_id'].apply(lambda x: 1 if pd.notnull(x) else 0)
del result_sql7['src_cust_id']

# SQL-8
ccp = pd.read_sql_table("stg_dce_cust_cmmnc_pref", engine, "dwh_stg")
syst_cmmnc_pref = ccp[ccp['is_actv'] == 1]
syst_cmmnc_pref = syst_cmmnc_pref.drop_duplicates(subset='cust_id')
syst_cmmnc_pref[['is_marketing', 'is_referral_t', 'is_cc_expire', 'is_usage_75', 'is_usage_90', 'is_usage_100',
                 'is_transaction_confirmation', 'is_roaming_zone_change', 'is_fair_data']] = 0
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 10000, 'is_marketing'] = syst_cmmnc_pref['is_slct'].max()
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 30000, 'is_referral_t'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 70000, 'is_cc_expire'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 110000, 'is_usage_75'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 110001, 'is_usage_90'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 110002, 'is_usage_100'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 50000, 'is_transaction_confirmation'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 90000, 'is_roaming_zone_change'] = 1
syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 40000, 'is_fair_data'] = 1

result_sql8 = syst_cmmnc_pref[['cust_id', 'is_marketing', 'is_referral_t', 'is_cc_expire', 'is_usage_75',
                               'is_usage_90', 'is_usage_100', 'is_transaction_confirmation',
                               'is_roaming_zone_change', 'is_fair_data']]

# SQL-9
try:
    pref = pd.read_sql_table("stg_dce_syst_cmmnc_pref", engine, "dwh_stg")
    syst_cmmnc_pref = pref[pref['is_actv'] == 1]
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 10000, 'is_marketing_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 30000, 'is_referral_t_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 70000, 'is_cc_expire_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 110000, 'is_usage_75_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 110001, 'is_usage_90_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 110002, 'is_usage_100_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 50000, 'is_transaction_confirmation_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 90000, 'is_roaming_zone_change_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref.loc[syst_cmmnc_pref['ntf_topic_id'] == 40000, 'is_fair_data_default'] = \
        syst_cmmnc_pref['is_slct'].max()
    syst_cmmnc_pref = syst_cmmnc_pref[['is_marketing_default', 'is_referral_t_default', 'is_cc_expire_default',
                                       'is_usage_75_default', 'is_usage_90_default', 'is_usage_100_default',
                                       'is_transaction_confirmation_default', 'is_roaming_zone_change_default',
                                       'is_fair_data_default']]
    result = pd.merge(result_sql7, result_sql8, how='left', left_on=['cust_id'], right_on=['cust_id'])
    result_sql9 = pd.merge(result, syst_cmmnc_pref, how='left', left_on=[1], right_on=[1])
    result_sql9['is_marketing'] = result_sql9['is_marketing'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_marketing_default'])
    result_sql9['is_referral_t'] = result_sql9['is_referral_t'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_referral_t_default'])
    result_sql9['is_cc_expire'] = result_sql9['is_cc_expire'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_cc_expire_default'])
    result_sql9['is_usage_75'] = result_sql9['is_usage_75'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_usage_75_default'])
    result_sql9['is_usage_90'] = result_sql9['is_usage_90'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_usage_90_default'])
    result_sql9['is_usage_100'] = result_sql9['is_usage_100'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_usage_100_default'])
    result_sql9['is_transaction_confirmation'] = result_sql9['is_transaction_confirmation'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_transaction_confirmation_default'])
    result_sql9['is_roaming_zone_change'] = result_sql9['is_roaming_zone_change'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_roaming_zone_change_default'])
    result_sql9['is_fair_data'] = result_sql9['is_fair_data'].apply(
        lambda x: x if pd.notnull(x) else syst_cmmnc_pref['is_fair_data_default'])
    result_sql9.to_sql("pandas_tmp_cust_9", engine, "dwh", index=False)
except:
    result_sql9 = pd.merge(result_sql7, result_sql8, how='left', left_on=['cust_id'], right_on=['cust_id'])

# SQL-10
au = pd.read_sql_table("stg_dce_apl_user", engine, "dwh_stg")
ll = pd.read_sql_table("stg_dce_lang", engine, "dwh_stg")
au.rename(columns={"party_id": "prty_id"}, inplace=True)
ll.rename(columns={"name": "pref_lang"}, inplace=True)
lang = pd.merge(au[['prty_id', 'pref_lang_id', 'ntf_pref_lang_id']], ll[['lang_id', 'pref_lang']], how='left',
                left_on=['pref_lang_id'], right_on=['lang_id'])
del lang['lang_id']
ll.rename(columns={"pref_lang": "ntf_pref_lang"}, inplace=True)
lang2 = pd.merge(lang, ll[['lang_id', 'ntf_pref_lang']], how='left',
                 left_on=['ntf_pref_lang_id'], right_on=['lang_id'])
del lang2['lang_id']
result_sql = pd.merge(result_sql9, lang2, how='left', left_on=['party_id'], right_on=['prty_id'])
result_sql10 = pd.merge(result_sql, au[['prty_id', 'st_id']], how='left', left_on=['party_id'], right_on=['prty_id'])
result_sql10['invalid_email'] = result_sql10['st_id'].apply(lambda x: 1 if (174 <= x <= 178) else 0)
del result_sql10['st_id']

# SQL-11
cst = pd.read_sql_table("dwd_pre_customer", engine, "dwh")
del result_sql10['prty_id_x'], result_sql10['prty_id_y']
cst_insert = cst.append(result_sql10, ignore_index=True)

# SQL-12
dwd = pd.read_sql_table("dwd_customer", engine, "dwh")
etl_date = datetime.datetime.now()
dwd.update(cst_insert)
dwd['etl_date'] = etl_date

# SQL-13
result = pd.merge(cst_insert['cust_id'], dwd, how='left', left_on=['cust_id'], right_on=['cust_id'])
result['exists'] = result['etl_date'].apply(lambda x: 1 if pd.notnull(x) else 0)
d = result.loc[result['exists'] == 0]
etl_date = datetime.datetime.now()
d['etl_date'] = etl_date
dwd_insert = dwd.append(d, ignore_index=True)
del dwd_insert['exists']

# SQL-14
dwd_hstr = pd.read_sql_table("dwd_hstr_customer", engine, "dwh")
dwd_hstr = dwd_hstr.loc[dwd_hstr['is_current_record'] == 1]
result_sql11 = dwd_insert[~(dwd_insert.isin(dwd_hstr).all(axis=1))]

# SQL-15
dwd_hstr = pd.read_sql_table("dwd_hstr_customer", engine, "dwh")
dwd_hstr = dwd_hstr.loc[dwd_hstr['is_current_record'] == 1]
result_sql12 = pd.merge(dwd_hstr, result_sql11['cust_id'], how='inner', left_on=['cust_id'], right_on=['cust_id'])
result_sql12['effective_to_date'] = result_sql11['udate_party']
result_sql12['is_current_record'] = 0
result_sql12['sys_effective_to_date'] = v_date

# SQL-16
result_sql11['isnull'] = result_sql11['effective_from_date'] = result_sql11['udate_party'].apply(lambda x: 1 if pd.notnull(x) else 0)
tmp111 = result_sql11.loc[result_sql11['isnull'] == 0]
indexNames = result_sql11[result_sql11['isnull'] == 0].index
result_sql11.drop(indexNames, inplace=True)
tmp111['udate_party'] = tmp111['cdate_party']
tmp11 = result_sql11.append(tmp111, ignore_index=True)
tmp11['effective_from_date'] = tmp11['udate_party']
tmp11['etl_date'] = datetime.datetime.now()
tmp11['effective_to_date'] = pd.NaT
tmp11['is_current_record'] = 1
tmp11['sys_effective_to_date'] = pd.NaT
tmp11['sys_effective_from_date'] = v_date
del tmp11['isnull']
dwd_hstr_insert = result_sql12.append(tmp11, ignore_index=True)

endTime = datetime.datetime.now()
print("BASLANGIC ZAMANI : ", v_date)
print("BITIS ZAMANI : ", endTime)
print("KOD UYGULAMA ZAMANI : ", endTime - v_date)