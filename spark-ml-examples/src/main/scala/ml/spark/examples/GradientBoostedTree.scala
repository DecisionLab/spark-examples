package ml.spark.examples

import azure.AzureUtilities._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}
import org.kohsuke.args4j.spi.StringArrayOptionHandler

import scala.collection.JavaConverters.seqAsJavaListConverter

object GBTCLIArgs {
  @org.kohsuke.args4j.Option(name = "-storage_account",
    depends = Array("-container", "-mount_point", "-secrets_blob_scope", "-secrets_blob_key"),
    usage = "Storage Account for mounting azure blobstore (Databricks Only)")
  var storage_account: String = null

  @org.kohsuke.args4j.Option(name = "-container",
    depends = Array("-storage_account", "-mount_point", "-secrets_blob_scope", "-secrets_blob_key"),
    usage = "Container for mounting azure blobstore (Databricks Only)")
  var container: String = null

  @org.kohsuke.args4j.Option(name = "-mount_point",
    depends = Array("-container", "-storage_account", "-secrets_blob_scope", "-secrets_blob_key"),
    usage = "Location to mount the azure blob too (Databricks Only)")
  var mount_point: String = null

  @org.kohsuke.args4j.Option(name = "-secrets_blob_scope",
    depends = Array("-container", "-mount_point", "-storage_account", "-secrets_blob_key"),
    usage = "Databricks Secret Scope which contains the Azure SAS")
  var secrets_blob_scope: String = null

  @org.kohsuke.args4j.Option(name = "-secrets_blob_key",
    depends = Array("-container", "-mount_point", "-storage_account", "-secrets_blob_scope"),
    usage = "Databricks Secret Key which contains the Azure SAS")
  var secrets_blob_key: String = null

  @org.kohsuke.args4j.Option(name = "-train",
    forbids = Array("-fit"),
    usage = "Flags this run to be a training run")
  var train: Boolean = false

  @org.kohsuke.args4j.Option(name = "-fit",
    forbids = Array("-train"),
    depends = Array("-model_location"),
    usage = "Flags the run to be a fitting run")
  var fit: Boolean = false

  @org.kohsuke.args4j.Option(name = "-label_column",
    depends = Array("-train"),
    usage = "The name of the label column (If Training)")
  var label_column: String = ""

  @org.kohsuke.args4j.Option(name = "-model_location",
    usage = "If training, location to output the trained model, if fitting, location of trained pipeline model")
  var model_location: String = null

  @org.kohsuke.args4j.Option(name = "-results_output_location",
    required = true,
    usage = "Results output location. If training this will contain the fitted results of 30% of the training data.")
  var results_output_location: String = ""

  @org.kohsuke.args4j.Option(name = "-input_location",
    required = true,
    usage = "Location of the data for training or fitting (Expects Parquet)")
  var input_location: String = ""

  @org.kohsuke.args4j.Option(name = "-feature_columns",
    required = true, handler = classOf[StringArrayOptionHandler],
    usage = "Space delimited list of the feature columns")
  var feature_columns: Array[String] = Array[String]()

}

/**
  * Example of a Spark Pipeline utilizing a gradient boosted tree for predictions.s
  */
object GradientBoostedTree {

  def parseCLIArgs(args: Array[String]) = {
    val parser = new CmdLineParser(GBTCLIArgs)
    try {
      parser.parseArgument(seqAsJavaListConverter(args.toSeq).asJava)

      println(s"storage_account: ${GBTCLIArgs.storage_account}  " +
        s"\ncontainer: ${GBTCLIArgs.container} " +
        s"\nmount_point: ${GBTCLIArgs.mount_point} " +
        s"\nsecrets_blob_scope: ${GBTCLIArgs.secrets_blob_scope}" +
        s"\nsecrets_blob_key: ${GBTCLIArgs.secrets_blob_key}" +
        s"\ntrain: ${GBTCLIArgs.train}" +
        s"\nfit: ${GBTCLIArgs.fit}" +
        s"\ninput_location: ${GBTCLIArgs.input_location}" +
        s"\nresults_output_location: ${GBTCLIArgs.results_output_location}" +
        s"\nmodel_location: ${GBTCLIArgs.model_location}" +
        s"\nfeature_columns: ${GBTCLIArgs.feature_columns}" +
        s"\nlabel_column: ${GBTCLIArgs.label_column}"
      )
    } catch {
      case e: CmdLineException => {
        parser.printUsage(System.out)
        throw e
      }
    }

    if (!(GBTCLIArgs.train || GBTCLIArgs.fit)) {
      throw new IllegalArgumentException("Must be training or fitting.")
    }
  }

  def main(args: Array[String]): Unit = {

    parseCLIArgs(args)

    val sparkSession = SparkSession
      .builder
      .appName("Gradient Boosted Tree")
      .getOrCreate()

    // Mount the blob into databricks if specified
    if (GBTCLIArgs.container != null) {
      mountBlobDatabricks(GBTCLIArgs.container, GBTCLIArgs.mount_point, GBTCLIArgs.storage_account,
        GBTCLIArgs.secrets_blob_scope, GBTCLIArgs.secrets_blob_key)
    }

    val data = sparkSession.read.parquet(GBTCLIArgs.input_location)

    var pipelineModel: PipelineModel = null

    var Array(trainingData, testData) = data.randomSplit(Array(0, 1), 123)

    if (GBTCLIArgs.train) {
      val Array(newTrainingData, newTestData) = data.withColumn("label", col(GBTCLIArgs.label_column)).randomSplit(Array(0.7, 0.3), 123)
      trainingData = newTrainingData
      testData = newTestData

      pipelineModel = train(trainingData)

      if (GBTCLIArgs.model_location != null) {
        pipelineModel.write.overwrite().save(GBTCLIArgs.model_location)
      }
    } else {
      pipelineModel = PipelineModel.load(GBTCLIArgs.model_location)
    }

    val predictions = pipelineModel.transform(testData)

    var predictionsOutput = GBTCLIArgs.results_output_location
    if (!predictionsOutput.endsWith("/")) {
      predictionsOutput = predictionsOutput + "/"
    }

    var flag = ""
    if (GBTCLIArgs.train) {
      flag = "_test_from_training"
    }

    val format = new java.text.SimpleDateFormat("dd-MM-yyyy_hh-mm-ss")
    predictions.write.parquet(GBTCLIArgs.results_output_location + format.format(new java.util.Date()) + flag)

    if (GBTCLIArgs.train) {
      evaluate(predictions, pipelineModel)
    }
  }

  def train(data: DataFrame): PipelineModel = {

    // Set up assembler to assemble the input columns into a vector
    val assembler = new VectorAssembler()
      .setInputCols(GBTCLIArgs.feature_columns)
      .setOutputCol("features")

    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, gbt))

    pipeline.fit(data)
  }

  def evaluate(data: DataFrame, model: PipelineModel) = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(data)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n ${gbtModel.toDebugString}")

  }
}
