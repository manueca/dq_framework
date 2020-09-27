
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import qa_framework_functions._
import qa_framework_initialize._
import org.apache.log4j.{Level, Logger}
import qaFrameWorkMl._

object qa_framework_transform{
    val log = Logger.getLogger(getClass.getName)
    log.info(s"Inside the transforms")
    println (s"Inside the transforms")
    def non_query_condition_transform(spark:SparkSession,
                                      dq_check_type:String,
                                      df_init:DataFrame,
                                      src_arg:DataFrame,
                                      dag_exec_dt:String,
                                      model_type:String,
                                      audit_metric_table:DataFrame) : DataFrame ={
        val df1 = src_arg.rdd.collect()
        var df=df_init
        for(i <- 0 until df1.length)
            {
                var id=df1(i).mkString(",")
                print ("\n id: "+id)
                var result_set=qualityParameterExtraction(spark,id,dag_exec_dt,audit_metric_table)

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
                var environment=result_set._12
                var team_name=result_set._13
                print ("\n Season_flg: "+season_flg)
                print ("\n canary_flag is :"+canary_flag)
                print ("\n query_temp is :"+query_temp,query_temp.trim.length)

                var ml_df=initializeMlDf(spark,id,table_name,dag_exec_dt)
                if (query_temp.trim.length ==0){
                  var kpi_val_df : DataFrame= getKpiValueNew(spark,query,environment)
                  kpi_val_df.registerTempTable("kpi_val_df")

                  if (model_type.length()==0){
                     ml_df = mlTransformIds(spark)
                  }
                  var ml_result_temp=ml_df.collect()
                  var ml_result=ml_result_temp(0)
                  var kpi_forecast=ml_result(3)
                  log.info(s"kpi_forecast is $kpi_forecast")
                  var actual_variance=ml_result(4)
                  log.info(s"actual_variance is $actual_variance")
                  var variance_percentage=ml_result(5)
                  log.info(s"variance_percentage is $variance_percentage")


                  var kpi_avg=get_average(spark,season_flg,id,dag_exec_dt,audit_metric_table)
                  //var kpi_avg=0
                  log.info(s"kpi_avg is $kpi_avg")
                  print ("\n variance_tolerance_limit : is "+variance_tolerance_limit)
                  var status = "Failed"
                  //var status = calculate_status(spark,kpi_val,kpi_avg,variance_tolerance_limit,dag_exec_dt,parent_id,model_type,variance_percentage)

                  //final_query="""select '$id' as id,
                  print ("\n. id :"+id+"\n kpi_avg is :"+kpi_avg+"\n status is : "+status)
                  var condition_to_check_temp=condition_to_check.replace("'","")
                  print (s"""condition_to_check_temp is $condition_to_check_temp""")
                  var final_query=s"""select '$id' as id,
                              '$full_tbl_scan' as full_tbl_scan,
                              '$variance_tolerance_limit' as variance_tolerance_limit,
                              '$condition_to_check_temp' as condition_to_check,
                              '$partition_col_nm' as partition_col_nm,
                              '$parent_id' as parent_id,
                              '$environment' as environment,
                              '$dq_check_type' as dq_check_type,
                              '$team_name' as team_name,
                              '$dag_exec_dt' as process_dt,
                              '$table_name' as table_name,
                              '$status' as status"""
                  var df_temp=spark.sql(final_query)
                  df_temp.registerTempTable("df_temp")
                  var df_final_temp=spark.sql(s"""select
                                                    a.id,
                                                    a.full_tbl_scan,
                                                    a.variance_tolerance_limit,
                                                    b.kpi_val,
                                                    a.condition_to_check_temp,
                                                    a.partition_col_nm,
                                                    a.parent_id,
                                                    a.environment,
                                                    a.dq_check_type,
                                                    a.team_name,
                                                    b.process_dt,
                                                    a.table_name,
                                                    a.status
                                                    from df_temp a
                                                    left outer join kpi_val_df b
                                                    on a.id=b.id
                                                    and a.process_dt=b.process_dt
                                                    """
                                                    )
                  print ("\n printing the final query ")
                  if (i==0){
                     df = df_final_temp
                  }else{
                     df = df.union(df_final_temp)
                  }
                  df.show(false)

                }
                else
                {
                println (s"query processing")
                }

        }
        return df
      }
      def query_condition_transform(spark:SparkSession,
                                        dq_check_type:String,
                                        id:String,
                                        dag_exec_dt:String,
                                        config_table:String) : DataFrame ={
      	  log.info(" Id IS $id")
      	  log.info(" dq_check_type IS $dq_check_type")

          var query=spark.sql(s"""select query from
                                    dev_eda_common.$config_table
                                    where id=$id""").rdd.collect().mkString(",")

          var query_final=query.replace("$dag_exec_dt",dag_exec_dt).replace("select ","select '$dq_check_type' as dq_check_type,")
          println (s"""Query is $query_final""")
          var df=spark.sql(query_final)
          return df
          }

  }
