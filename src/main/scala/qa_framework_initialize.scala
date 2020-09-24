import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.log4j.{Level, Logger}

object qa_framework_initialize {
    val log = Logger.getLogger(getClass.getName)
    def initialize_dataframe(spark:SparkSession,environment:String): (DataFrame,DataFrame) ={
        var final_query=s"""select '0' as id,
                    '0' as kpi_val,
                    'Y' as full_tbl_scan,
                    '0' as variance_tolerance_limit,
                    '' as condition_to_check,
                    '' as partition_col_nm,
                    '' as parent_id,
                    ''+'$environment' as environment,
                    '' as team_name,
                    '' as process_dt,
                    '' as table_name"""
        var df=spark.sql(final_query)
	      var df_result=spark.sql(final_query)
	      df.show()
        log.info(s"Initializing the dataframe")
        return (df,df_result)
    }
}
