import com.typesafe.config.Config
import scala.collection.JavaConverters._

class Settings(config: Config) extends Serializable {
    val configTable = config.getString("config.detail.table-name")
    val configDatabase = config.getString("config.detail.database-name")
}
