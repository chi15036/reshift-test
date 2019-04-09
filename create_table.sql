CREATE EXTERNAL TABLE gga_schema.develop_feature_tw_bank_traffic_second(
  num_886 varchar, 
  has_traffic_pattern_second varchar)
PARTITIONED BY ( 
  search_dt varchar)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'escapeChar'='\\', 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://gogolook-rd/yangchi/redshift'
table properties ('skip.header.line.count'='1');
