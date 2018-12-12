# glue-test-map-reduce
Testing Glue on EMR with a simple Map Reduce

## Running the jar in EMR 

```
HADOOP_CLASSPATH=/usr/lib/hive/lib/*:/usr/lib/hive-hcatalog/share/hcatalog/*:/etc/hive/conf:$(hadoop classpath) hadoop jar glue-test-map-reduce.jar com.godaddy.dataplatform.Application
```


## Reproduction steps
1. Create S3 buckets `s3://test-aiyangar/read/` and `s3://test-aiyangar/write/` and drop the `read.csv` in read folder.
2. Create glue database `c360_glue_database`
3. Create tables 
###### Read Table
```
CREATE EXTERNAL TABLE `c360_glue_database.read`(
  `id` string COMMENT 'from deserializer', 
  `name` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://test-aiyangar/read/'
TBLPROPERTIES (
  'classification'='csv')
 ```
###### Write Table
 ```
 CREATE EXTERNAL TABLE `c360_glue_database.write`(
  `original_name` string COMMENT 'from deserializer', 
  `value` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://test-aiyangar/write/'
TBLPROPERTIES (
  'classification'='csv')
 ```
4. Spin up EMR cluster copy the jar and run.

## Stacktrace for the above Application

```
Exception in thread "main" java.io.IOException: NoSuchObjectException(message:c360_glue_database.read table not found)
    at org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:97)
    at org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:71)
    at com.godaddy.dataplatform.Application.run(Application.java:101)
    at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)
    at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:90)
    at com.godaddy.dataplatform.Application.main(Application.java:73)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:498)
    at org.apache.hadoop.util.RunJar.run(RunJar.java:239)
    at org.apache.hadoop.util.RunJar.main(RunJar.java:153)
Caused by: NoSuchObjectException(message:c360_glue_database.read table not found)
    at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$get_table_req_result$get_table_req_resultStandardScheme.read(ThriftHiveMetastore.java:55064)
    at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$get_table_req_result$get_table_req_resultStandardScheme.read(ThriftHiveMetastore.java:55032)
    at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$get_table_req_result.read(ThriftHiveMetastore.java:54963)
    at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:86)
    at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.recv_get_table_req(ThriftHiveMetastore.java:1563)
    at org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client.get_table_req(ThriftHiveMetastore.java:1550)
    at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getTable(HiveMetaStoreClient.java:1344)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:498)
    at org.apache.hadoop.hive.metastore.RetryingMetaStoreClient.invoke(RetryingMetaStoreClient.java:169)
    at com.sun.proxy.$Proxy4.getTable(Unknown Source)
    at org.apache.hive.hcatalog.common.HCatUtil.getTable(HCatUtil.java:180)
    at org.apache.hive.hcatalog.mapreduce.InitializeInput.getInputJobInfo(InitializeInput.java:105)
    at org.apache.hive.hcatalog.mapreduce.InitializeInput.setInput(InitializeInput.java:88)
    at org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(HCatInputFormat.java:95)
    ... 11 more
   
   
   
   
   
   
