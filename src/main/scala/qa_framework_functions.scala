
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}

object qa_framework_functions {
	val log = Logger.getLogger(getClass.getName)

	def getKpiValue(spark:SparkSession,query:String,environment:String) : Double ={
		var df_val=0.0
		println ("environment is "+environment)
		if (environment.toLowerCase()=="hive"){
			var df_val=spark.sql(query).first().getAs[Double](0)
		}else{
			var df_val=spark.sql(query).first().getAs[Double](0)
		}
		return df_val
	}
	def getKpiValueNew(spark:SparkSession,query:String,environment:String) : DataFrame ={
		var df_val=spark.sql(s"""select 'test'""")
		if (environment.toLowerCase()=="hive"){
			df_val=spark.sql(query)
		}else{
			df_val=spark.sql(query)
		}
		return df_val
	}


	def calculate_status(spark:SparkSession,kpi_val:Double,kpi_avg:Double,variance_tolerance_limit:String,dag_exec_dt:String,parent_id:String,model_type:String,variance_percentage:String) : String ={
			if (variance_tolerance_limit.toInt==100 && kpi_val<=0.0){
				return "Failed"
			}
			if ( ( ((Math.abs(kpi_val - kpi_avg)/kpi_avg) *100) > variance_tolerance_limit.toInt) ){
				print ("inside 1")
				log.info(s"Returning failure because of kpi_average   variance $kpi_avg is greater than tolerance limit $variance_tolerance_limit compared to kpi value")
				return "Failed"
			}
			if (parent_id.length >1){
				var parent_id_val=spark.sql(s"""select cast(coalesce(kpi_val,0.0) as double) as kpi_val from dev_eda_common.mpa_audit_metric_table  where id='$parent_id' and process_dt='$dag_exec_dt'""").first().getAs[Double](0)
				if (parent_id_val != kpi_val && variance_tolerance_limit.toInt >0 ){

						if (((Math.abs(kpi_val - parent_id_val)/parent_id_val) *100)>variance_tolerance_limit.toInt && variance_tolerance_limit.toInt != 0 ){
								log.info(s"Returning failure because of parent id  variance $parent_id_val is greater than tolerance limit $variance_tolerance_limit compared to kpi value")
								return "Failed"
						}
				}
				if (parent_id_val != kpi_val && variance_tolerance_limit.toInt == 0 ){
					log.info(s"Returning failure because of parent id value not equal to kpi value")
					return "Failed"
				}
			}
			if (variance_percentage > variance_tolerance_limit && model_type.length()>0 ){
					log.info(s"Returning failure because of prediction variance $variance_percentage is greater than tolerance limit $variance_tolerance_limit ")
					return "Failed"
			}
			return "Success"
		}

	def get_average(spark:SparkSession,
									season_flg:String,
									id:String,
									dag_exec_dt:String,
									audit_metric_table:DataFrame) : Double ={
		audit_metric_table.registerTempTable("audit_metric_table")
		var kpi_avg=0
		var ret_kpi_avg=0.0
		print ("\n before check ")
		if (season_flg=='Y') {
			print ("inside season condition")

		}
		if (season_flg=='N') {
			print ("\n  inside season condition with N")

		}
		if (season_flg.trim()=="") {
			print ("\n  inside season condition with N")

		}
		else {
			print ("\n  inside  else ")
			var comp_dt_val = dag_exec_dt
			print ("comp_dt_val is :  "+comp_dt_val)
			var compar_dt = LocalDate.parse(comp_dt_val, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
			var mod_compar_dt=compar_dt.minusDays(season_flg.toInt)
			print ("\n mod_compar_dt is :  "+mod_compar_dt,id)
			print ("\n id is :  "+mod_compar_dt,id)

			var kpi_avg=spark.sql(s"""select cast(coalesce(avg(coalesce(kpi_val,0.0)),0.0) as double) as kpi_val
									 from
									   audit_metric_table
									   where id='$id' and process_dt >= '$mod_compar_dt'""").first().getAs[Double](0)
			print (s"""\n kpi_avg for id $id is $kpi_avg""")
			if (kpi_avg >0){
				 ret_kpi_avg=kpi_avg
			} else{
				ret_kpi_avg=0.0
			}
					print (" \n kpi_avg is "+ret_kpi_avg)
		}
		return ret_kpi_avg
	}

	def qualityParameterExtraction( spark:SparkSession,id:String ,
																	dag_exec_dt:String,
																	audit_metric_table:DataFrame,
																	settings:Settings) :
																	(String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String,
																	String)  = {
		audit_metric_table.registerTempTable("audit_metric_table")
		val config_table_name=settings.configTable
		val database_name=settings.configDatabase

		spark.sql(s"""select * from $database_name.$config_table_name""").printSchema()
		val reslt_without_query=spark.sql(s"""select coalesce(table_name,''),
	 												 coalesce(season_flag,'') as season_flag,
	 												 coalesce(kpi,''),
	 												 coalesce(environment,''),
	 												 coalesce(full_tbl_scan,''),
	 												 coalesce(canary_flag,''),
	 												 coalesce(variance_tolerance_limit,''),
	 												 coalesce(condition_to_check,'') ,
	 												 coalesce(partition_col_nm,''),
	 												 coalesce(query,''),
	 												 coalesce(parent_id,''),
	 												 coalesce(team_name,'')
	 										from $database_name.$config_table_name
	 										where id='$id'""").collect()

		println (reslt_without_query)
	 	val table_name = reslt_without_query.map(x => x.get(0)).mkString(",")
	 	var season_flag = reslt_without_query.map(x => x.get(1)).mkString(",")
	 	val kpi = reslt_without_query.map(x => x.get(2)).mkString(",")
	 	val environment = reslt_without_query.map(x => x.get(3)).mkString(",")
	 	var full_tbl_scan = reslt_without_query.map(x => x.get(4)).mkString(",")
	 	val canary_flag = reslt_without_query.map(x => x.get(5)).mkString(",")
	 	val variance_tolerance_limit = reslt_without_query.map(x => x.get(6)).mkString(",")
	 	val condition_to_check = reslt_without_query.map(x => x.get(7)).mkString(",")
	 	val partition_col_nm = reslt_without_query.map(x => x.get(8)).mkString(",")
	 	val query_temp = reslt_without_query.map(x => x.get(9)).mkString(",")
	 	val parent_id = reslt_without_query.map(x => x.get(10)).mkString(",")
	 	val team_name = reslt_without_query.map(x => x.get(11)).mkString(",")
	 	//print ("full_tbl_scan"+full_tbl_scan,"partition_col_nm"+partition_col_nm)
		print ("\n  partition_col_nm IS :"+partition_col_nm)
		print ("\n  full_tbl_scan IS :"+full_tbl_scan)
		print ("\n  condition_to_check IS :"+condition_to_check)
		print ("\n  After condition to check")
	 	var query = "select cast("+kpi+" as double) as kpi_val from "+table_name
		var df=spark.sql(s"""select '$table_name'""")
	 	print ("\n  full_tbl_scan IS :"+full_tbl_scan)
	 	if (full_tbl_scan=='Y'){
			df=spark.sql(s"""select * from $table_name""").cache()
			df.registerTempTable("df")
	 		query = s"""select '$id' as id,'$dag_exec_dt' as process_dt, cast("+kpi+" as double) as kpi_val from df"""
	 	}else{
			df=spark.sql(s"""select * from $table_name where $partition_col_nm >='$dag_exec_dt'""").cache()
			df.registerTempTable("df")
		}

	 	if (condition_to_check.length >1 && partition_col_nm.length <=0 && full_tbl_scan !='Y' ) {

	 		query = s"""select '$id' as id,$partition_col_nm as process_dt,cast($kpi as double) as kpi_val  from df where $condition_to_check group by $partition_col_nm"""
	 		print ("\n  inside where condition to check \n")
	 		}
	 	print ("partition_col_nm.length :"+partition_col_nm.length ,"full_tbl_scan:"+full_tbl_scan)

	 	if (partition_col_nm.length >1 && full_tbl_scan !="Y" && condition_to_check.length >1 ){

	 		query = s"""select '$id' as id,$partition_col_nm as process_dt,cast($kpi as double) as kpi_val from df where $condition_to_check and $partition_col_nm >= '$dag_exec_dt' group by $partition_col_nm """
	 		print ("\n  inside where condition to check and partition column condition \n")

	 	}
	 	if (partition_col_nm.length >1 && full_tbl_scan !="Y" && condition_to_check.length <=0 ){

	 		query = s"""select '$id' as id,$partition_col_nm as process_dt,cast($kpi  as double) as kpi_val from $table_name where  $partition_col_nm >= '$dag_exec_dt' group by $partition_col_nm"""
	 		print ("\n  inside partition column condition \n")
	 	}
	 	else{
	 		if (query_temp.length>0){
	 			query=query
	 		}
	 	}
		 return (season_flag,query,table_name,kpi,full_tbl_scan,canary_flag,variance_tolerance_limit,condition_to_check,partition_col_nm,query_temp,parent_id,environment,team_name)
		}

	def statusCalculation( spark:SparkSession ,
												 query_temp:String,
												 df_temp:DataFrame,
												 kpi_val_df:DataFrame,
												 model_type:String,
												 ml_df:DataFrame) :
												 DataFrame  = {
			println ("Printing dataframe outcomes")
			df_temp.show()
			kpi_val_df.show()
			var df_final=spark.sql("""select 'test'""")
			df_temp.registerTempTable("df_temp")
			kpi_val_df.registerTempTable("kpi_val_df")
			var query_temp=s"""select
	                    a.id,
	                    a.full_tbl_scan,
	                    a.variance_tolerance_limit,
	                    b.kpi_val,
	                    a.condition_to_check,
	                    a.partition_col_nm,
	                    a.parent_id,
	                    a.environment,
	                    a.dq_check_type,
	                    a.team_name,
											a.kpi_avg,
	                    b.process_dt,
	                    a.table_name,
	                    case when length(trim(model_type))==0 and ((cast(b.kpi_val as double)- cast(a.kpi_avg as double))/cast(b.kpi_val as double))*100 < cast(variance_tolerance_limit as double) then 'SUCCESS'
	                         else 'FAILED'
	                         end as status
	                    from df_temp a
	                    full outer join kpi_val_df b
	                    on a.id=b.id"""
			var df_final_temp=spark.sql(query_temp)
			df_final_temp.registerTempTable("df_final_temp")

			df_final_temp.show()
			println(s"""Query is : $query_temp""")
			if (model_type.length()!=0){
					ml_df.registerTempTable("ml_df")
					println(s"""ML dataframe is shown below""")
					ml_df.show()
					ml_df.printSchema()
					query_temp=s"""select
			                    a.id,
			                    a.full_tbl_scan,
			                    a.variance_tolerance_limit,
			                    a.kpi_val,
			                    a.condition_to_check,
			                    a.partition_col_nm,
			                    a.parent_id,
			                    a.environment,
			                    a.dq_check_type,
			                    a.team_name,
													a.kpi_avg,
			                    b.process_dt,
			                    a.table_name,
			                    case when  cast(variance_percentage as double) < cast(variance_tolerance_limit as double) then 'SUCCESS'
			                         else 'FAILED'
			                         end as status
			                    from df_final_temp a
			                    full outer join ml_df b
			                    on a.id=b.id
													and a.process_dt=b.process_dt"""
					df_final=spark.sql(query_temp)
			}
			else{
			df_final=df_final_temp
			}
			println ("Final query printing")
			df_final.show(100,false)
			return df_final
	}
}
