
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.scalatest.{BeforeAndAfter, FunSuite}

class Testqa_framework_functions extends FunSuite with BeforeAndAfter {
  private val kpi_val=100;
  private val kpi_avg=100;
  private val variance_tolerance_limit="100";
  private val dag_exec_dt="2020-01-01";
  private val parent_id="";
  private val out = "Success";
  var spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
 /*
  before {
    var spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  }
  */
  test("Calculate status test") {
   
    assertResult(out)(qa_framework_functions.calculate_status(spark:SparkSession,kpi_val:Double,kpi_avg:Double,variance_tolerance_limit:String,dag_exec_dt:String,parent_id:String))
  }

  after {
    println("test completed")
  }
}
