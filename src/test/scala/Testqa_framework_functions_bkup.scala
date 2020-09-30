
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.types.{StringType, LongType, StructField, StructType,DoubleType,IntegerType}
import org.apache.spark.sql.Row
/*
 def schemaGenerator(types: Array[String], cols: Array[String]) :StructType =  {
   val datatypes = types.map {
     case "String" => StringType
     case "Long" => LongType
     case "Double" => DoubleType
     case "Int" => IntegerType
     // Add more types here based on your data.
     case _ => StringType
   }
   return StructType(cols.indices.map(x => StructField(cols(x), datatypes(x))).toArray)
 }

 def dataframeGenerator(schema:StructType,data:Tuple) : DataFrame ={

   val expectedDF = spark.createDataFrame(
     spark.sparkContext.parallelize(someData),schema)
   return expectedDF
 }
 */


class Testqa_framework_functions extends FunSuite with BeforeAndAfter {
  private val kpi_val=100;
  private val kpi_avg=100;
  private val variance_tolerance_limit="100";
  private val dag_exec_dt="2020-01-01";
  private val parent_id="";
  private val out = "Success";
  private val model_type="LIR";
  private val variance_percentage="100";
  //private val query_temp="";
  //private val df_temp=
  //private val kpi_val_df=
  //private val ml_df=
  var spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
 /*
  before {
    var spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  }
  */
  test("Calculate status test") {

    assertResult(out)(qa_framework_functions.calculate_status(spark:SparkSession,kpi_val:Double,kpi_avg:Double,variance_tolerance_limit:String,dag_exec_dt:String,parent_id:String,model_type:String,variance_percentage:String))
  }
/*
  test("Testing statusCalculation") {

    var df1=qa_framework_functions.statusCalculation(spark:SparkSession,
                                                              query_temp:String,
                                                              df_temp:DataFrame,
                                                              kpi_val_df:DataFrame,
                                                              model_type:String,
                                                              ml_df:DataFrame)
    var df2=df_out_statusCalculation
    df1.assertEquals(df2)
  }
*/

  after {
    println("test completed")
  }

}
