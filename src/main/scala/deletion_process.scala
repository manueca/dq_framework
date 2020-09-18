//spark-submit --driver-memory 47G --conf spark.delta.merge.repartitionBeforeWrite=true --executor-memory 47G --packages io.delta:delta-core_2.11:0.4.0 --deploy-mode "client" --class "deletion_process" qaframework_2.11-1.0.11.jar
//nohup spark-submit --driver-memory 100G --executor-memory 47G --conf spark.delta.merge.repartitionBeforeWrite=true  --conf "spark.debug.maxToStringFields=100" --conf "spark.driver.extraJavaOptions=-Xms20g" --conf "spark.driver.maxResultSize=5g" --conf "spark.delta.merge.repartitionBeforeWrite=true"  --packages io.delta:delta-core_2.11:0.4.0 --class "deletion_process" qaframework_2.11-1.0.11.jar &
// df1.write.mode("overwrite").format("parquet").save('s3://zz-testing/jcher2/blacklist_modified/')
import io.delta.tables._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;


object deletion_process {
	def main(args : Array[String]) : Unit = {
        var spark = SparkSession.builder().appName("delta").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").enableHiveSupport().getOrCreate()
		print(args(0));
		print(args(1));
		
                val treatment_s3_location = "s3://zz-testing/jcher2/bm_poc/";
		val blacklist_s3_location = "s3://zz-testing/jcher2/blacklist_modified/";
		val treatment_field = "prod_cd";
		val blacklist_field = "prodt_cd";
		val treatment_alias = "treatment"
		val blacklist_alias = "blacklist"
		spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
		val delta_table_2 = spark.read.format("delta").load(treatment_s3_location)
		delta_table_2.printSchema()

		val delta_table_1 = DeltaTable.forPath(spark, treatment_s3_location)
	
		//spark.sql("ANALYZE TABLE delta_table_1 COMPUTE STATISTICS FOR COLUMNS prodt_cd")
		val blacklist_table_df = spark.read.parquet(blacklist_s3_location)
		val start_time = Calendar.getInstance().getTimeInMillis()
		val treatment_partition_col="supergeo_cd";

		val blacklist_partition_col="supergeo_cd";

		//val condition = String.format("%s.%s = %s & %s.%s = %s.%s", treatment_alias,treatment_partition_col,blacklist_alias,blacklist_partition_col,treatment_alias, treatment_field, blacklist_alias, blacklist_field)
		//val condition = s"${treatment_alias}.${treatment_field} = ${blacklist_alias}.${blacklist_field} AND  ${treatment_alias}.${treatment_partition_col} = ${blacklist_alias}.${blacklist_partition_col}" 
		val condition = s"${treatment_alias}.${treatment_field} = ${blacklist_alias}.${blacklist_field} AND  ${treatment_alias}.${treatment_partition_col} = 'EU'" 
		print (condition)
		delta_table_1.alias(treatment_alias).merge(blacklist_table_df.alias(blacklist_alias), condition).whenMatched().delete().execute()
		val end_time = Calendar.getInstance().getTimeInMillis()
		val duration = end_time - start_time
		print(TimeUnit.MILLISECONDS.toSeconds(duration))
	}
	

}



