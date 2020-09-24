
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import qa_framework_functions._
import org.apache.log4j.{Level, Logger}
object qa_framework_transform{
    val log = Logger.getLogger(getClass.getName)
    log.info(s"Inside the transforms")
    println (s"Inside the transforms")
    def non_query_condition_transform(spark:SparkSession,environment:String,df_init:DataFrame,src_arg:DataFrame,dag_exec_dt:String) : DataFrame ={
        val df1 = src_arg.rdd.collect()
        var df=df_init
        for(i <- 0 until df1.length)
            {
                var id=df1(i).mkString(",")
                print ("\n id: "+id)
                var result_set=quality_parameter_extraction(spark,id,dag_exec_dt)
                var season_flg=result_set._1
                var query=result_set._2
                var table_name=result_set._3
                var kpi=result_set._4
                var full_tbl_scan=result_set._5
                var canary_flag=result_set._6
                var variance_tolerance_limit=result_set._7
                var condition_to_check=result_set._8
                var partition_col_nm=result_set._9
                var query_temp=result_set._10
                var parent_id=result_set._11
                var team_name=result_set._13
                print ("\n Season_flg: "+season_flg)
                print ("\n canary_flag is :"+canary_flag)
            print ("\n query_temp is :"+query_temp,query_temp.trim.length)

            if (query_temp.trim.length ==0){
                  var kpi_val= get_kpi_val(spark,query,environment)

                  print ("KPI VAL is :"+kpi_val)
                  //var kpi_avg=get_average(spark,season_flg,id,dag_exec_dt)
            var kpi_avg=0
                  print ("\n variance_tolerance_limit : is "+variance_tolerance_limit)
                  var status = "Failed"
            //var status = calculate_status(spark,kpi_val,kpi_avg,variance_tolerance_limit,dag_exec_dt,parent_id)
                  //final_query="""select '$id' as id,
                  print ("\n. id :"+id+"\n kpi_avg is :"+kpi_avg+"\n status is : "+status+"\n kpi_val:"+kpi_val)
                  var final_query=s"""select '$id' as id,
                              '$kpi_val' as kpi_val,
                              '$full_tbl_scan' as full_tbl_scan,
                              '$variance_tolerance_limit' as variance_tolerance_limit,
                              '$condition_to_check' as condition_to_check,
                              '$partition_col_nm' as partition_col_nm,
                              '$parent_id' as parent_id,
                              '$environment'+'_canary' as environment,
                              '$team_name' as team_name,
                              '$dag_exec_dt' as process_dt,
                              '$table_name' as table_name"""
                  var df_temp=spark.sql(final_query)
                  print ("\n printing the final query ")
                  if (i==0){
                     df = df_temp
                  }else{
                     df = df.union(df_temp)
                }
                  df.show(false)

            }
            else
            {
              print ("query processing")
            }

        }
        return df
      }
      def query_condition_transform(spark:SparkSession,
                                        environment:String,
                                        id:String,
                                        dag_exec_dt:String) : DataFrame ={
	  log.info(" Id IS $id")
	  log.info(" Environment IS $environment")
	 
          var query=spark.sql("""select query from
                                    dev_eda_common.eda_quality_framework_config_ddb
                                    where id='$id'""").rdd.collect().mkString(",")
          var query_final=query.replace("$dag_exec_dt",dag_exec_dt)
          println (s"""Query is $query""")
          var df=spark.sql(query_final)
          return df
          }

  }
