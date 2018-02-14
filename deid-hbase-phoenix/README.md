Two examples for creating unique IDs in Spark, one using Apache Phoenix and one using Apache HBase.  The inspiration for these examples are from the need to create new IDs for De-identification of patient records.

## Build
This project builds using Maven:
- `mvn clean package`

The resulting jar file will be located in the `./target/` directory, but later commands in this document omit any directory structure.
- `./target/uber-deid-hbase-phoenix-0.1-SNAPSHOT.jar`

## Phoenix example
A [Phoenix Sequence](https://phoenix.apache.org/sequences.html) is used to generate auto-incrementing ID numbers.  This example assumes it is provided with a list that only contains de-duplicated Patient IDs that need a new DeID.

### Setup sequence in Phoenix
1. `phoenix-sqlline`
1. `CREATE SCHEMA some_schema;`
1. `CREATE SEQUENCE some_schema.some_sequence;`

### Run Spark job
- `/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --files /usr/hdp/current/hbase-client/conf/hbase-site.xml --conf spark.driver.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar --conf spark.executor.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar --class "SparkPhoenixDeid" uber-deid-hbase-phoenix-0.1-SNAPSHOT.jar some_schema.some_sequence`

## HBase example
A look-up table of IDs are stored in HBase, and new IDs are generated (UUID) automatically when one is not found. Since this is not currently using any form of transaction support, normal parallel operation in Spark could cause duplicate IDs to be generated.

This example assumes it is provided with raw patient event data -- some of the Patient IDs may exist in the lookup table, while others will need new De-IDs.

### Setup table in HBase
1. `hbase shell`
1. `create 'deidTable', 'deidCf'`
1. `put 'deidTable', 'p0001', 'deidCf', 'aaaa-1234'`
1. `put 'deidTable', 'p0002', 'deidCf', 'bbbb-1234'`
1. `put 'deidTable', 'p0003', 'deidCf', 'cccc-1234'`
1. `scan 'deidTable'`

### Run Spark job
- `/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --files /usr/hdp/current/hbase-client/conf/hbase-site.xml --class "SparkHbaseDeid" uber-deid-hbase-phoenix-0.1-SNAPSHOT.jar deidTable deidCf`

## Output
By default, the example jobs write output to HDFS for demonstration:
- `hadoop fs -cat sampleDeid/part*`

These output files will need to be cleaned up between jobs
- `hadoop fs -rm -r sampleDeid
`
