import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/**
  * /usr/hdp/current/spark2-client/bin/spark-submit \
  * --master yarn \
  * --files /usr/hdp/current/hbase-client/conf/hbase-site.xml \
  * --conf spark.driver.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar \
  * --conf spark.executor.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client.jar \
  * --class "SparkPhoenixDeid" \
  * uber-spark-hbase-deid-0.1-SNAPSHOT.jar
  *
  * java.sql.SQLException: ERROR 726 (43M10):  Inconsistent namespace mapping properites.. Cannot initiate connection as SYSTEM:CATALOG is found but client does not have phoenix.schema.isNamespaceMappingEnabled enabled
  * NOTE: for Phoenix, the hbase-site.xml file MUST be copied into spark/conf
  * cp /usr/hdp/current/hbase-client/conf/hbase-site.xml /usr/hdp/current/spark2-client/conf/
  */

// singleton to provide single Phoenix connection per executor
object PhoenixConnectionSingleton {
  @transient private var connection: Connection = _

  def getConnectionInstance(jdbcUrl: String): Connection = {
    if (connection == null) {
      // need a connection to Phoenix for each executor
      connection = DriverManager.getConnection(jdbcUrl)
      print("connection created")
    }
    connection
  }

}

object SparkPhoenixDeid {
  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: spark-submit ... <Schema.Sequence>
           |  <Schema.Sequence> Phoenix schema.sequence to read
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(sequenceSchema) = args


    val sparkConf = new SparkConf()
    // hbase master should be read from hbase-site.xml
    //sparkConf.set("hbase.master", "hdp-ambari.internal.decisionlab.io"+":16000")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // This list must only contain de-duplicated Patient IDs that need a new DeID
    val patientInfoRdd = spark.sparkContext.parallelize(Array(
      ("1", "p0001"),
      ("2", "p0002"),
      ("3", "p0003"),
      ("4", "p0004"),
      ("5", "p0005"),
      ("6", "p0006"),
      ("7", "p0007"),
      ("8", "p0008")
    ))

    import spark.implicits._
    var patientInfoDf = patientInfoRdd.toDF("id", "pid")

    // function that queries HBase for DeID matching patient ID
    def getNextDeid() : Integer = {
      var deid: Integer = null

      // hbase master and znode.parent will be read from hbase-site.xml
      val jdbcUrl = "jdbc:phoenix" //:hdp-ambari.internal.decisionlab.io:2181:/hbase-unsecure"
      val connection = PhoenixConnectionSingleton.getConnectionInstance(jdbcUrl)

      // Phoenix Sequence query
      val sequenceQuery = "SELECT NEXT VALUE FOR " + sequenceSchema
      val pstmt = connection.prepareStatement(sequenceQuery)
      val rs: ResultSet = pstmt.executeQuery

      // get first result, and read what should be the only value
      rs.next()
      deid = rs.getInt(1)

      if (rs.next()){
        throw new SQLException("Programming Error: ResultSet should have only returned one value!")
      }

      deid
    }

    // add new column to patient event data with DeID
    patientInfoDf = patientInfoDf.withColumn("deid", lit(getNextDeid))

    // write out for review
    patientInfoDf.rdd.saveAsTextFile("sampleDeid")

  }
}
