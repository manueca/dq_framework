import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .getOrCreate()
  }
}
