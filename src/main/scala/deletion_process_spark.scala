
//nohup spark-submit --driver-memory 12G --executor-memory 3G --conf spark.delta.merge.repartitionBeforeWrite=true --executor-cores 8 --conf spark.sql.files.maxPartitionBytes=53687091  --conf "spark.debug.maxToStringFields=100" --conf "spark.driver.extraJavaOptions=-Xms3g" --conf "spark.driver.maxResultSize=1g" --conf "spark.delta.merge.repartitionBeforeWrite=true" --deploy-mode cluster --packages io.delta:delta-core_2.11:0.4.0 --class "deletion_process_spark" qaframework_2.11-1.0.11.jar &
//nohup spark-submit --driver-memory 12G --executor-memory 2G --conf spark.delta.merge.repartitionBeforeWrite=true --executor-cores 8 --conf spark.sql.files.maxPartitionBytes=23687091  --conf "spark.debug.maxToStringFields=100" --conf "spark.driver.extraJavaOptions=-Xms3g" --conf "spark.driver.maxResultSize=1g" --conf "spark.delta.merge.repartitionBeforeWrite=true" --deploy-mode cluster --packages io.delta:delta-core_2.11:0.4.0 --class "deletion_process_spark" qaframework_2.11-1.0.11.jar &
//final version on 7 TB cluster nohup spark-submit --driver-memory 2G --executor-memory 19G   --deploy-mode cluster --packages io.delta:delta-core_2.11:0.4.0 --class "deletion_process_spark" qaframework_2.11-1.0.11.jar &


import io.delta.tables._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import sys.process._


object deletion_process_spark {
	def main(args : Array[String]) : Unit = {
        var spark = SparkSession.builder().appName("delta").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").enableHiveSupport().getOrCreate()
        val treatment_s3_location = "s3://zz-testing/jcher2/po_eu/";
		val blacklist_s3_location = "s3://zz-testing/jcher2/blacklist_modified/";
		val treatment_field = "prodt_cd";
		val blacklist_field = "prodt_cd";
		val treatment_alias = "treatment"
		val blacklist_alias = "blacklist"
	
		//spark.sql("ANALYZE TABLE delta_table_1 COMPUTE STATISTICS FOR COLUMNS prodt_cd")
		val blacklist_table_df = spark.read.parquet(blacklist_s3_location)
		val treatment_table_df = spark.read.parquet(treatment_s3_location)
		val start_time = Calendar.getInstance().getTimeInMillis()
		val treatment_partition_col="supergeo_cd";
		
		val blacklist_partition_col="supergeo_cd";
                blacklist_table_df.registerTempTable("blacklist_table_df")
		treatment_table_df.registerTempTable("treatment_table_df")
		//val treatment_table_df_filter=spark.sql("""select * from treatment_table_df""")
		//treatment_table_df_filter.registerTempTable("treatment_table_df_filter")
		val df=spark.sql("""select * from treatment_table_df where   prodt_cd not in (select prodt_cd from blacklist_table_df)  """)
		df.write.partitionBy("retailer_id","snapshot_dt").mode("overwrite").format("parquet").save("/data/po_base_new1_out/")
		val end_time = Calendar.getInstance().getTimeInMillis()
		val duration = end_time - start_time
		print(TimeUnit.MILLISECONDS.toSeconds(duration))
	}
	

}




//sudo vim /etc/spark/conf/log4j.properties
// log4j.rootCategory=DEBUG,console
// log location : s3://ngap2-us-e1-platform/emr/platform/log//j-1639OGOW4H2PK/containers/application_1599574907506_0012/

df.write.partitionBy("account_type","supergeo_cd","retailer_id","inv_snapshot_dt").mode("overwrite").format("parquet").save("/data/bm/")