//spark-submit --packages net.snowflake:snowflake-jdbc:3.4.2,net.snowflake:spark-snowflake_2.11:2.2.8,com.amazon.emr:emr-dynamodb-hadoop:4.6.0,com.amazon.emr:emr-dynamodb-hive:4.8.0 --master local[2] --deploy-mode "client" --class "qa_framework_main" qaframework_2.11-1.0.11.jar N Y

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import qa_framework_functions._
import qa_framework_transform._
import qa_framework_initialize._
import org.apache.log4j.{Level, Logger}

object qa_framework_main {
	val log = Logger.getLogger(getClass.getName)

	def main(args : Array[String]) : Unit = {
		log.info(s"Inside the Main Function")
		println (s"Inside the Main Function")
		var spark = SparkSession.builder().enableHiveSupport().getOrCreate()
		spark.sparkContext.setLogLevel("WARN")
		spark.conf.set("hive.mapred.mode", "nonstrict")
		spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		spark.conf.set("hive.exec.dynamic.partition", "true")
		spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
		spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
		val table_name = "dsmsca_staging.ip_doms_inventory_daily_base_t"
		val canary_value=args(0)
		var query_condition=args(1)
		var id=""
		if (query_condition != "Y"){
		    id = s"""select distinct id from dev_eda_common.eda_quality_framework_config_ddb where table_name='${table_name}' and canary_flag='$canary_value' and length(trim(query)) =0 """
		}
		else{
		    id = s"""select distinct id from dev_eda_common.eda_quality_framework_config_ddb where table_name='${table_name}' and canary_flag='$canary_value' and length(trim(query)) >0"""
		}
		print ("\n query_condition is :"+id)
		var src_tbl = spark.sql(id)
		src_tbl.cache()
		val df1 = src_tbl.rdd.collect()
		if (df1.length ==0){
				println ("NO IDS for the table")
				System.exit(1)
		}
		val dag_exec_dt="2020-08-17"
		var  environment="dq_check_noquery"
		if (canary_value =="N" && query_condition.length !=0){
			environment="dq_check_query";
		}
		if (canary_value =="Y" && query_condition.length !=0){
			environment="canary_query";
		}
		if (canary_value =="Y" && query_condition.length ==0){
			environment="canary_noquery";
		}
		println(df1)
		println(s"""ENVIRONMENT IS $environment""")

		src_tbl.show()

		var (df,df_result)=initialize_dataframe(spark,environment)
		if (environment=="dq_check_noquery" || environment=="canary_noquery"){
		    df_result=qa_framework_transform.non_query_condition_transform(spark,environment,df,src_tbl,dag_exec_dt)
		}
		else{
		    df_result=qa_framework_transform.query_condition_transform(spark,environment,id,dag_exec_dt)
		}
    df_result.write.partitionBy("team_name","environment","process_dt","table_name").mode("overwrite").parquet("s3://zz-testing/jcher2/qa_testing/")
	}

}
