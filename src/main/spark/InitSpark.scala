import org.apache.spark.sql.SparkSession
import java.util.logging.{Level, Logger}

trait InitSpark {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val spark = SparkSession.builder()
    .appName("Inicio Spark")
    .master("local[1]")
    .getOrCreate()

}
