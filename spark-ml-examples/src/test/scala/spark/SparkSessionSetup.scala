package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionSetup {
  def withSparkSession(testMethod: (SparkSession) => Any): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Test")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      testMethod(spark)
    }
    finally spark.stop()
  }
}
