import scala.beans.BeanInfo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


object qaFrameWorkMl {
	val log = Logger.getLogger(getClass.getName)
  def linearRegressionModel(): LinearRegression = {
      var ml = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      return ml
  }
  def logisticReggressionModel(): LogisticRegression = {
      var ml = new LogisticRegression().setMaxIter(1).setRegParam(0.3).setElasticNetParam(0.8)
      return ml
  }
  def RandomForestRegressorModel():RandomForestRegressor = {
      val ml = (new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("features"))
      return ml

  }
  def GradientBoostedTreeRegressorModel():RandomForestRegressor = {
      val ml = (new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("features"))
      return ml

  }
	def mlTransformIds(spark:SparkSession,
										kpi_val_df:DataFrame,
										dag_exec_dt:String,
										id:String,
										audit_metric_table:DataFrame): DataFrame = {
      var model_type="GBT"

      //val dag_exec_dt="2020-07-01"
      val table_name="dsmsca_processed.inventory_pipeline_sc_agg"
      val DateIndexer = new StringIndexer().setInputCol("process_dt").setOutputCol("DateCat")
      //var ml= new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

      case class LabeledDocument(table_name:String,id: String, process_dt: String, label: Double)
      /*var query=s"""select table_name,id,process_dt,cast(kpi_val as double) as label
                                  from dev_eda_common.mpa_audit_metric_table
                                  where table_name='$table_name' and id='$id' """
			*/
			println("displaying audit_metric_table")
			audit_metric_table.show()
			println (s"ID is $id")
			audit_metric_table.registerTempTable("audit_metric_table")
			var query=(s"""select distinct id,process_dt ,cast(kpi_val as double) as label from audit_metric_table where id='$id'""")
			println (s""" Query on audit metric table is $query""")

			val training = spark.sql(query)
      //training.as[LabeledDocument]
			println ("Displaying training")
			training.show()
      val indexed = DateIndexer.fit(training).transform(training)


      var pipeline = new Pipeline()
      val tokenizer = new Tokenizer().setInputCol("process_dt").setOutputCol("words")
      val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")

      if (model_type.toUpperCase=="LIR"){
					println ("Selected LIR for modeling")
          var ml_LinearRegression=linearRegressionModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_LinearRegression))
      }
      if (model_type.toUpperCase()=="LGR"){
				  println ("Selected LGR for modeling")
          var ml_random_forest=logisticReggressionModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_random_forest))
      }
      // This section of code is for Random forest regressor
      if (model_type.toUpperCase()=="RFR"){
					println ("Selected RFR for modeling")
          var ml_RandomForest=RandomForestRegressorModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_RandomForest))
      }
      if (model_type.toUpperCase()=="GBT"){
					println ("Selected GBT for modeling")
          var ml_GBT=GradientBoostedTreeRegressorModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_GBT))
      }
			indexed.show()
      val model = pipeline.fit(indexed.toDF())
      import spark.implicits._
      case class Document(id: String, process_dt: String)
      //val test = spark.sparkContext.parallelize(Seq(Document(s"$id", s"$dag_exec_dt")))
      val test1 = Seq((s"$id", s"$dag_exec_dt")).toDF("id", "process_dt")
			println ("Displaying kpi_val_df")
			kpi_val_df.show()
			kpi_val_df.registerTempTable("kpi_val_df")
			training.registerTempTable("training")
			var test = spark.sql(s"""select distinct id,process_dt from kpi_val_df where process_dt >'$dag_exec_dt' """)


			println ("Printing Test data set for ML model")
			test.show()
      var final_df=model.transform(test)
			println ("Printing Outcome from Model")
      final_df.show()
      final_df.registerTempTable("final_df")
      query=s"""select 		a.id,
                          a.process_dt,
                          cast(label as double) as kpi_val,
													prediction as forecast_val,
                          prediction-cast(label as double) as variance ,
                          ((prediction-cast(label as double))/label)*100 as variance_percentage
                                  from training a
                                  left outer join final_df b
                                  on a.id=b.id
                                  and a.process_dt=b.process_dt
                                  """
      var df=spark.sql(query)
			println ("""Printing Schema of ML dataframe""")
			df.printSchema()
			df.show()
      return df
  }

}
