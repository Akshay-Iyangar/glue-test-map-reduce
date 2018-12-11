# glue-test-map-reduce
Testing Glue on EMR with a simple Map Reduce

## Running the jar in EMR 

```
HADOOP_CLASSPATH=/usr/lib/hive/lib/*:/usr/lib/hive-hcatalog/share/hcatalog/*:/etc/hive/conf:$(hadoop classpath) hadoop jar glue-test-map-reduce.jar com.godaddy.dataplatform.Application
```
