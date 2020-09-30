//aws s3 cp s3://zz-testing/jcher2/qaframework_2.11-1.0.11.jar
//spark-submit --deploy-mode cluster --packages net.snowflake:snowflake-jdbc:3.4.2,net.snowflake:spark-snowflake_2.11:2.2.8,com.amazon.emr:emr-dynamodb-hadoop:4.6.0,com.amazon.emr:emr-dynamodb-hive:4.8.0,com.typesafe:config:1.2.1,MrPowers:spark-fast-tests:0.20.0-s_2.12 --conf "spark.sql.crossJoin.enabled=true" --class "qa_framework_main" qaframework_2.11-1.0.11.jar Y N dev
//spark-shell --packages org.scalatest:scalatest_2.13:3.2.2,com.holdenkarau:spark-testinge_2.12:2.4.5_0.14.0

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import qa_framework_functions._
import qa_framework_transform._
import qa_framework_initialize._
import org.apache.log4j.{Level, Logger}
import com.typesafe.config._

object qa_framework_main {
	val log = Logger.getLogger(getClass.getName)

	def main(args : Array[String]) : Unit = {
		log.info(s"Inside the Main Function")
		println (s"Inside the Main Function")
		var spark = SparkSession.builder().enableHiveSupport().getOrCreate()
		spark.sparkContext.setLogLevel("INFO")
		spark.conf.set("hive.mapred.mode", "nonstrict")
		spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		spark.conf.set("hive.exec.dynamic.partition", "true")
		spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
		spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
		val table_name = "dsmsca_processed.inventory_pipeline_sc_agg"
		val model_type="LIR"
		val canary_value=args(0)
		var query_condition=args(1)
		var env=args(2)
		var conf_file_name=env+"_application.conf"


		val conf: Config = ConfigFactory.load(conf_file_name)
		val settings: Settings = new Settings(conf)

		println (s"Settings.configTable")

		val config_table_name=settings.configTable
		val database_name=settings.configDatabase

		var id=""
		if (query_condition != "Y"){
		    id = s"""select distinct id from $database_name.$config_table_name where table_name='${table_name}' and canary_flag='$canary_value' and length(trim(query)) =0 """
		}
		else{
		    id = s"""select distinct id from $database_name.$config_table_name where table_name='${table_name}' and canary_flag='$canary_value' and length(trim(query)) >0"""
		}
		log.info(s"query used to get id : $id")
		var src_tbl = spark.sql(id)
		src_tbl.cache()
		val df1 = src_tbl.rdd.collect()
		println (s"\nIDs are $df1 ")
		if (df1.length ==0){
				println ("NO IDS for the table")
				System.exit(1)
		}
		val dag_exec_dt="2020-08-17"
		var  dq_check_type="dq_check_noquery"
		println(canary_value)
		println(query_condition)
		println (s"canary_value is $canary_value and query_condition is $query_condition")
		log.info(s"canary_value is $canary_value and query_condition is $query_condition")
		if (canary_value =="N" && query_condition =="Y"){
			dq_check_type="dq_check_query";
		}
		if (canary_value =="Y" && query_condition =="Y"){
			dq_check_type="canary_query";
		}
		if (canary_value =="Y" && (query_condition.length ==0 || query_condition =="N")){
			dq_check_type="canary_noquery";
		}
		// Initialize and cache the AuditMetricTable
		var audit_metric_table=cacheAuditMetricTable(spark,s"$database_name.mpa_audit_metric_table",table_name)
		audit_metric_table.cache()
		println(s"""dq_check_type IS $dq_check_type""")

		src_tbl.show()

		var (df,df_result)=initialize_dataframe(spark,dq_check_type)
		if (dq_check_type=="dq_check_noquery" || dq_check_type=="canary_noquery"){

		    df_result=qa_framework_transform.non_query_condition_transform(spark,dq_check_type,df,src_tbl,dag_exec_dt,model_type,audit_metric_table,settings)
				println ("Printing the final data frame results")
				df_result.cache()
				df_result.show()
		}
		else{
		    df_result=qa_framework_transform.query_condition_transform(spark,dq_check_type,id,dag_exec_dt,config_table_name)
		}
    df_result.write.partitionBy("team_name","dq_check_type","process_dt","table_name").mode("overwrite").parquet("s3://zz-testing/jcher2/qa_testing/")
	}

}
