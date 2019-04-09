alter table gga_schema.develop_feature_tw_bank_traffic_second add
partition(search_dt='2019-04-05') 
location 's3://gogolook-rd/yangchi/redshift/';
