import psycopg2
con=psycopg2.connect("dbname=dev host=witcher-test.creydll8nevp.ap-northeast-1.redshift.amazonaws.com port=5439 user=awsuser password=Aa26298077")
cursor = con.cursor()
cursor.execute("""
unload ('
--- feature_combine_features_include_special_num
--- source: witcher_fin.staging_feature_tw_bank_combine_features_exclude_special_num,
---         witcher_fin.staging_feature_tw_bank_special_num

SELECT
    raw_data.num_886 AS num_886,
    raw_data.uid AS uid,
    last_activity_range,
    activity_rank1and2,
    using_frequency,
    z_using_frequency,
    bad_history,
    source_bad_history,
    has_social_activity,
    social_activity_name_count,
    has_region_3,
    has_region_4,
    has_region_above5,
    report_tag,
    special_tag,
    yp_source,
    yp_label,
    special_type,
    spam_type,
    source_spam,
    is_special_loan_owner,
    is_agency_owner,
    is_nominee_owner,
    is_highpaid_owner,
    is_spec_career_owner,
    is_debt_collect_owner,
    is_black_owner,
    is_scalper_owner,
    has_network_0_30,
    has_network_30_60,
    has_network_0_180,
    reachable_0_365,
    reachable_0_30,
    reachable_30_60,
    has_fraud_pattern_1,
    has_fraud_pattern_2,
    has_fraud_pattern_3,
    has_fraud_pattern_6,
    has_fraud_pattern_7,
    has_fraud_pattern_8,
    z_frequency_day,
    z_frequency_week,
    z_frequency_month,
    has_traffic_pattern_hour,
    has_traffic_pattern_minute,
    has_traffic_pattern_second,
    is_invalid_number,
    is_verify,
    whitelist_in_6month,
    z_whitelist_in_6month,
    whitelist_out_6month,
    z_whitelist_out_6month,
    agency_in_6month,
    z_agency_in_6month,
    agency_out_6month,
    z_agency_out_6month,
    special_loan_in_6month,
    z_special_loan_in_6month,
    special_loan_out_6month,
    z_special_loan_out_6month,
    nominee_in_6month,
    z_nominee_in_6month,
    nominee_out_6month,
    z_nominee_out_6month,
    spec_career_in_6month,
    z_spec_career_in_6month,
    spec_career_out_6month,
    z_spec_career_out_6month,
    scalper_in_6month,
    z_scalper_in_6month,
    scalper_out_6month,
    z_scalper_out_6month,
    pawnshop_in_6month,
    z_pawnshop_in_6month,
    pawnshop_out_6month,
    z_pawnshop_out_6month,
    highpaid_in_6month,
    z_highpaid_in_6month,
    highpaid_out_6month,
    z_highpaid_out_6month,
    debt_collect_in_6month,
    z_debt_collect_in_6month,
    debt_collect_out_6month,
    z_debt_collect_out_6month,
    black_in_6month,
    z_black_in_6month,
    black_out_6month,
    z_black_out_6month,

    report_tag_v2,
    yp_source_v2,
    is_pawnshop_owner,
    is_private_lending_owner,
    private_lending_in_6month,
    z_private_lending_in_6month,
    private_lending_out_6month,
    z_private_lending_out_6month

FROM

    (SELECT *
    FROM witcher_fin.staging_feature_tw_bank_combine_features_exclude_special_num
    WHERE search_dt = ''2019-04-07''
    ) AS raw_data

    LEFT JOIN

    (SELECT *
    FROM witcher_fin.staging_feature_tw_bank_special_num
    WHERE search_dt = ''2019-04-07''
    ) AS special_num

    ON raw_data.num_886 = special_num.num_886
    AND raw_data.search_dt = special_num.search_dt
') 
to 's3://gogolook-rd/yangchi/redshift/search_dt=2019-04-07/'
iam_role 'arn:aws:iam::473024607515:role/witcherfin-api'
HEADER
CSV; 

""")
