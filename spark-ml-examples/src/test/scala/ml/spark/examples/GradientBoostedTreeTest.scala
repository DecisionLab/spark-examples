package ml.spark.examples

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec, Assertions}
import org.scalatest.concurrent.Eventually
import spark.SparkSessionSetup

class GradientBoostedTreeTest extends WordSpec
  with Matchers
  with Eventually
  with BeforeAndAfterAll
  with SparkSessionSetup {

  "GradientBoostedTreeTest" should {
    " parse cli args with just input " in {

      val args_valid_input = Array("-train", "-input_location", "/mnt/data", "-results_output_location", "/mnt/data",
        "-feature_columns", "these are the input columns", "-label_column", "this_is_the_label_column")

      GradientBoostedTree.parseCLIArgs(args_valid_input)
      GBTCLIArgs.input_location shouldBe "/mnt/data"
    }

    "fail to parse args with missing required input " in {
      val caught =
        intercept[org.kohsuke.args4j.CmdLineException] {
          val args_missing_required = Array[String]()
          GradientBoostedTree.parseCLIArgs(args_missing_required)
        }
      assert(caught.getMessage.equals("Option \"-feature_columns\" is required"))
    }

    "fail to parse args with missing dependency args " in {
      val caught =
        intercept[org.kohsuke.args4j.CmdLineException] {
          val args_missing_required = Array[String]("-train", "-input_location", "/mnt/data", "-results_output_location", "/mnt/data",
            "-feature_columns", "these are the input columns", "-label_column", "this_is_the_label_column", "-storage_account", "my_account")
          GradientBoostedTree.parseCLIArgs(args_missing_required)
        }
      println(caught.getMessage())
      assert(caught.getMessage.equals("option \"-storage_account\" requires the option(s) [-container, -mount_point, -secrets_blob_scope, -secrets_blob_key]"))
    }
  }
}
