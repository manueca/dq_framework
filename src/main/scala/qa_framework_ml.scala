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
	def mlTransformIds(spark:SparkSession,kpi_val_df:DataFrame): DataFrame = {
      var model_type="GBT"
      val id="a20191108181137_62bf55ca_e21e_460a_a1z";
      val dag_exec_dt="2020-07-01"
      val table_name="dsmsca_processed.inventory_pipeline_sc_agg"
      val DateIndexer = new StringIndexer().setInputCol("process_dt").setOutputCol("DateCat")
      //var ml= new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

      case class LabeledDocument(table_name:String,id: String, process_dt: String, label: Double)
      var query=s"""select table_name,id,process_dt,cast(kpi_val as double) as label
                                  from dev_eda_common.mpa_audit_metric_table
                                  where table_name='$table_name' and id='$id' """
      val training = spark.sql(query)
      //training.as[LabeledDocument]
      val indexed = DateIndexer.fit(training).transform(training)


      var pipeline = new Pipeline()
      val tokenizer = new Tokenizer().setInputCol("process_dt").setOutputCol("words")
      val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")

      if (model_type.toUpperCase=="LIR"){
          var ml_LinearRegression=linearRegressionModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_LinearRegression))
      }
      if (model_type.toUpperCase()=="LGR"){

          var ml_random_forest=logisticReggressionModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_random_forest))
      }
      // This section of code is for Random forest regressor
      if (model_type.toUpperCase()=="RFR"){

          var ml_RandomForest=RandomForestRegressorModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_RandomForest))
      }
      if (model_type.toUpperCase()=="GBT"){

          var ml_GBT=GradientBoostedTreeRegressorModel()
          pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, ml_GBT))
      }
      val model = pipeline.fit(indexed.toDF())
      import spark.implicits._
      case class Document(id: String, process_dt: String)
      //val test = spark.sparkContext.parallelize(Seq(Document(s"$id", s"$dag_exec_dt")))
      //val test = Seq((s"$id", s"$dag_exec_dt")).toDF("id", "process_dt")
			kpi_val_df.registerTempTable("kpi_val_df")
			var test = spark.sql("""select distinct id,process_dt from kpi_val_df""")
      var final_df=model.transform(test)
      final_df.show()
      final_df.registerTempTable("final_df")
      query=s"""select table_name,
                          a.id,
                          a.process_dt,
                          cast(kpi_val as double) as label,
													prediction as forecast_val,
                          prediction-cast(kpi_val as double) as variance ,
                          ((prediction-cast(kpi_val as double))/kpi_val)*100 as variance_percentage
                                  from dev_eda_common.mpa_audit_metric_table a
                                  left outer join final_df b
                                  on a.id=b.id
                                  and a.process_dt=b.process_dt
                                  where table_name='$table_name' and a.id='$id' and a.process_dt='$dag_exec_dt'"""
      var df=spark.sql(query)
      return df
  }

}
