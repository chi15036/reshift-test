Redshift

`效能測試`

**Select * from gga_schema.”gga_callog” limit 10**

3 dc2.large - 1m50s
20 dc2.large - 20s
4 dc2.8xlarge - 15s
20 dc2.8xlarge - 16s ?

**select * from gga_schema."gga_calllog"**
**WHERE dt between '2018-01-01' and '2018-02-01'**

Athena: 3m29s
20 dc2.8xlarge -  11m23s  (結果提早出現！？)

**feature_traffic_second**

Athena: 1m15s
5 dc2.8xlarge: 6m8s
10 dc2.8xlarge: 5m52s
12 dc2.8xlarge: 6m10s
20 dc2.8xlarge: 5m59s

**feature_combine_features_include_special_num**

Athena: 23m17s
10 dc2.8xlarge: 26m35s
12 dc2.8xlarge: 26m40s
20 dc2.8xlarge: 27m20s


**1xfeature_combine_features_include_special_num** 
**9xfeature_traffic_second**

10 dc2.8xlarge: 
feature_traffic_second 平均變 9-10min 完成
Cpu 40%-50%
最後完成時間 26m
10 dc2.large: 
feature_combine_features_include_special_num  => abort
CPU 100%


**(Unload) feature_traffic_second**
10 dc2.8xlarge: 40.66s
10 dc2.large: 10min
3 dc2.8xlarge: 1m30s
3 dc2.8xlarge(queue slot=10): 1m14s

**(Unload) feature_combine_features_include_special_num**
10 dc2.8xlarge: 52.51s !!!!!
3 dc2.8xlarge(queue slot=10): 58.72s

**(Unload) 1xfeature_combine_features_include_special_num** 		        **9xfeature_traffic_second**
10 dc2.large:  oom
3 dc2.8xlarge(queue slot=10): 11min
max Cpu 80%
3 dc2.8xlarge: 15min 只跑完一個停掉
10 dc2.8xlarge: 6min
10 dc2.8xlarge(queue slot=10): 3m30s

`Unload 參數`
Limit 要寫subquery (https://docs.aws.amazon.com/en_us/redshift/latest/dg/r_UNLOAD.html)

`結論:`
	1. 覺得用10個 dc2.8xlarge 處裡 case8 還蠻綽綽有餘，如果跑三-四個國家，同時是跑12-16個 query，10個 dc2.8xlarge 蠻夠用的，只是 group 要再 tunning 

`WLM:`

* 每個 queue slot 數越少，每個 query 分到的 memory 越多 (推薦 15 以下)
推薦把運算少的放在 slot 較多的 queue，反之把運算多的放在 slot 較少的 queue
* user_group
create group admin_group with user awsuser // 創立 user_group
select * from pg_group; => 看所有 user_group
* query_group
Set query_group to 'Monday';
		SET select * from category limit 1; //創立 query group
select query, pid, substring, elapsed, label
		from svl_qlog where label ='Monday' //查看 query_group
	

`UDF:`
select * from pg_proc // list all UDF function


`Pricing`
	機器費用加上 Athena 花費 (spectrum 是分開算)
Athena 一天花費約10T(production + staging) = 50 USD
一年: 50 x 365 = 18250 USD
RI:
10 dc2.8xlarge 
一年期: 331800 USD
三年期: 153870 USD
10 dc2.large
一年期: 16450 USD
三年期: 9620 USD
沒 RI
maximum = 15 node (6美/hr) x 4hr = 360 USD / day
minimum = 10 node x 2hr = 120 USD /day

`OTHERS`
Create table 可以設定 'compression_type’，減少 query 費用
將 scan 量相當的 feature 放在同樣的



