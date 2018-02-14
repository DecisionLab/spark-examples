import java.util.UUID

import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, col}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * /usr/hdp/current/spark2-client/bin/spark-submit \
  * --master yarn \
  * --files /usr/hdp/current/hbase-client/conf/hbase-site.xml \
  * --class "SparkHbaseDeid" \
  * uber-spark-hbase-deid-0.1-SNAPSHOT.jar
  */

// singleton to provide single HBase connection per executor
object HbaseTableSingleton {
  @transient private var table: Table = _

  def getTableInstance(tableName: String): Table = {
    if (table == null) {
      // need a connection to HBase for each executor
      val hbaseConf = HBaseConfiguration.create()
      // zookeeper quorum should be read from hbase-site.xml
      //hbaseConf.set("hbase.zookeeper.quorum", "hdp-ambari.internal.decisionlab.io")
      val connection = ConnectionFactory.createConnection(hbaseConf)
      table = connection.getTable(TableName.valueOf(tableName))
      print("connection created")
    }
    table
  }

}

object SparkHbaseDeid {
  def main(args: Array[String]) = {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: spark-submit ... <Table> <CF>
           |  <Table> Table in HBase to read
           |  <CF> Column Family in HBase to read
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(tableName, columnFamily) = args

    val sparkConf = new SparkConf()
    // hbase master should be read from hbase-site.xml
    //sparkConf.set("hbase.master", "hdp-ambari.internal.decisionlab.io"+":16000")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // sample patient event data
    val patientInfoRdd = spark.sparkContext.parallelize(Array(
      ("1", "p0001"),
      ("2", "p0002"),
      ("3", "p0003"),
      ("4", "p0004"),
      ("5", "p0003"),
      ("6", "p0001"),
      ("7", "p0005"),
      ("8", "p0006"),
      ("9", "p0001"),
      ("10", "p0005"),
      ("11", "p0007")
    ))

    import spark.implicits._
    var patientInfoDf = patientInfoRdd.toDF("id", "pid")

    // function that queries HBase for DeID matching patient ID
    def queryDeid = (id: String) => {
      var deid: String = null

      val table = HbaseTableSingleton.getTableInstance(tableName)

      val deidResult = table.get(new Get(Bytes.toBytes(id)))
      val details = deidResult.getValue(Bytes.toBytes(columnFamily), null)

      if (details != null) {
        // ID exists in Lookup - return value
        deid = Bytes.toString(details)
      } else {
        // TODO: would need Transaction support (Tephra ?)
        // or checkAndPut (Hbase 2.0) or checkAndMutate (Hbase 3.0)

        // ID does not exist.  need new DeID added to table
        deid = UUID.randomUUID().toString
        val put = new Put(Bytes.toBytes(id))
        put.addColumn(Bytes.toBytes(columnFamily), null, Bytes.toBytes(deid))
        table.put(put)
      }

      deid
    }
    val deidUdf = udf(queryDeid)

    // add new column to patient event data with DeID
    patientInfoDf = patientInfoDf.withColumn("deid", deidUdf(col("pid")))

    // write out for review
    patientInfoDf.rdd.saveAsTextFile("sampleDeid")
  }
}
