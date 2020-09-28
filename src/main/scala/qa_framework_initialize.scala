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
    def initializeMlDf(spark:SparkSession,id:String,table_name:String,dag_exec_dt:String):(DataFrame)={
        var query=s"""select '$table_name' as table_name,
                        '$id' as id,
                        '$dag_exec_dt' as process_dt,
                        0 as kpi_val,
                        0.0 as variance ,
                        0.0 as variance_percentage"""
        var df=spark.sql(query)
        return df

    }
    def cacheAuditMetricTable(spark:SparkSession,audit_table:String,table_name:String):DataFrame = {
        var query=s"""select * from $audit_table where table_name='$table_name'"""
        println(s"""query to select the audit metric table is :$query""")
        var audit_metric_table=spark.sql(query)
        println("Showing audit metric table")
        audit_metric_table.cache()
        audit_metric_table.show()
        audit_metric_table.printSchema()
        return audit_metric_table
    }
}
