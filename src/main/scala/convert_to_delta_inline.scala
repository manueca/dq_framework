//spark-submit --driver-memory 100G --executor-memory 47G --conf spark.delta.merge.repartitionBeforeWrite=true  --conf "spark.debug.maxToStringFields=100" --conf "spark.driver.extraJavaOptions=-Xms20g" --conf "spark.driver.maxResultSize=5g" --conf "spark.delta.merge.repartitionBeforeWrite=true"  --packages io.delta:delta-core_2.11:0.4.0 --class "convert_to_delta_inline" qaframework_2.11-1.0.11.jar s3://zz-testing/jcher2/bm_inv/
import io.delta.tables._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrame ;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


object convert_to_delta_inline {
	def main(args : Array[String]) : Unit = {
        var spark = SparkSession.builder().appName("delta").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").enableHiveSupport().getOrCreate()
	//spark.sql("select * from dsmsca_staging.ip_bm_inventory_daily_base_t").show()
        val treatment_partition_schema = "account_type string,supergeo_cd string,retailer_id string,snapshot_dt string";
	val treatment_location=args(0);
        val treatment_identifier = String.format("parquet.`%s`",treatment_location);
        DeltaTable.convertToDelta(spark,treatment_identifier,treatment_partition_schema)
        }
	

}

