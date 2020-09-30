import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import org.apache.spark.sql.types.{StringType, LongType, StructField, StructType,DoubleType,IntegerType}
import org.apache.spark.sql.Row

class qa_framework_functionsSpec

    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._
  //-----------------------------------------------------------------------
  // This below section is for data set creation and variable declaration
  //-----------------------------------------------------------------------
  val audit_metric_data = Seq(
    Row(71.0, "2020-01-01","a20191219225319_bcaec291_a48e_4dd9_b0z")
  )

  val audit_metric_schema = List(
    StructField("kpi_val", DoubleType, true),
    StructField("process_dt", StringType, true),
    StructField("id", StringType, true)
  )

  val input_df = spark.createDataFrame(
    spark.sparkContext.parallelize(audit_metric_data),
    StructType(audit_metric_schema)
  )

  val df_temp=(Seq(
    ("a20191219225319_bcaec291_a48e_4dd9_b0z","N", "10",72.0,"",
    "process_dt","","hive","canary_noquery","dsm","71","2020-01-01","test","LIR")
    ).toDF("id","full_tbl_scan", "variance_tolerance_limit","kpi_val",
          "condition_to_check","partition_col_nm","parent_id","environment",
          "dq_check_type","team_name","kpi_avg","process_dt","table_name","model_type"))

  val ml_df= (Seq(
    ("a20191219225319_bcaec291_a48e_4dd9_b0z","2020-01-01", 72.0,71,2,5)).toDF("id","process_dt", "kpi_val","forecast_val","variance","variance_percentage"))


  val model_type="LIR"
  val query_temp=""


  val kpi_val_df_expected_data = Seq(
    Row("a20191219225319_bcaec291_a48e_4dd9_b0z", "2020-01-01",71.0)
  )

  val kpi_val_df_expected_schema = List(
    StructField("id", StringType, true),
    StructField("process_dt", StringType, true),
    StructField("kpi_val", DoubleType, true)
  )

  val kpi_val_df_expected_df= spark.createDataFrame(
    spark.sparkContext.parallelize(kpi_val_df_expected_data),
    StructType(kpi_val_df_expected_schema)
  )


// Building df_final data frame from the StatusCalculation method
  /*
  val df_final_expected_df= (Seq(
    ("a20191219225319_bcaec291_a48e_4dd9_b0z","N", "10",71.0,"","process_dt","","hive",
    "canary_noquery","dsm","71","2020-01-01","test",71.0,"SUCCESS","SUCCESS")
  ).toDF("id","full_tbl_scan","variance_tolerance_limit", "kpi_val",
    "condition_to_check","partition_col_nm","parent_id","environment","dq_check_type",
    "team_name","kpi_avg","process_dt","table_name","forecast_val","status","reason_for_failure"))
    df_final_expected_df.write.mode("overwrite").parquet("file:/Users/jcher2/test/")
 */
 val df_final_expected_df= spark.read.parquet("/Users/jcher2/Documents/jerry_git/dq_framework/src/test/resources/df_final_expected.parquet")

 //-----------------------------------------------------------------------
 // This below section is running the SBT tests.
 //-----------------------------------------------------------------------


  describe(".getKpiValueNew") {

    it("get the data from audit metric table") {

      input_df.registerTempTable("input_df")
      val query=s"""select id, process_dt,sum(kpi_val)	as kpi_val
                  from input_df group by id,process_dt """
      val environment="hive"
      println ("before running the assert")
      val actualDF = qa_framework_functions.getKpiValueNew(spark,query,environment)
      input_df.show()
      actualDF.show()
      println ("after running show")

      assertSmallDatasetEquality(actualDF, kpi_val_df_expected_df,ignoreNullable = true)

      }
    }

    describe(".statusCalculation") {

      it("Calculating the status for a specific metric") {

        val actualDF = qa_framework_functions.statusCalculation(spark,
                                                                query_temp,
                                                                df_temp,
                                                                kpi_val_df_expected_df,
                                                                model_type,ml_df)
        df_final_expected_df.show(100,false)
        actualDF.show(100,false)

        assertSmallDatasetEquality(actualDF, df_final_expected_df,ignoreNullable = true)

      }

    }




}
