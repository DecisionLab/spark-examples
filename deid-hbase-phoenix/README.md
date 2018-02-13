
## Phoenix
A [Phoenix Sequence](https://phoenix.apache.org/sequences.html) is used to generate auto-incrementing ID numbers.

### Setup sequence in Phoenix
1. `phoenix-sqlline`
1. `CREATE SCHEMA some_schema;`
1. `CREATE SEQUENCE some_schema.some_sequence;`

### Run Spark job
- `/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --files /usr/hdp/current/hbase-client/conf/hbase-site.xml --conf spark.driver.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar --conf spark.executor.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar --class "SparkPhoenixDeid" uber-spark-deid-examples-0.1-SNAPSHOT.jar some_schema.some_sequence`

## HBase
A look-up table of IDs are stored in HBase, and new IDs are generated (UUID) automatically when one is not found. Since this is not currently using any form of transaction support, normal parallel operation in Spark could cause duplicate IDs to be generated.

### Setup table in HBase
1. `hbase shell`
1. `create 'deidTable', 'deidCf'`
1. `put 'deidTable', 'p0001', 'deidCf', 'aaaa-1234'`
1. `put 'deidTable', 'p0002', 'deidCf', 'bbbb-1234'`
1. `put 'deidTable', 'p0003', 'deidCf', 'cccc-1234'`
1. `scan 'deidTable'`

### Run Spark job
- `/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --files /usr/hdp/current/hbase-client/conf/hbase-site.xml --class "SparkHbaseDeid" uber-spark-deid-examples-0.1-SNAPSHOT.jar tableName columnFamily`

## Output
By default, the example jobs write output to HDFS for demonstration:
- `hadoop fs -cat sampleDeid/part*`

These output files will need to be cleaned up between jobs
- `hadoop fs -rm -r sampleDeid
`