import org.apache.spark.SparkFiles
object ejercicio_mym extends  InitSpark {


  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    /*leer el csv*/
    val urlfile = "https://raw.githubusercontent.com/databricks/LearningSparkV2/master/chapter2/scala/data/mnm_dataset.csv"
    spark.sparkContext.addFile(urlfile)

    val df = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("file:///" + SparkFiles.get("mnm_dataset.csv"))

    /*todas las agregaciones para cada color de M&M para cada estado*/
    df
      .groupBy($"State",$"Color")
      .sum("Count").as("Cantidad por estado y color")
      .show()

    /*aquellas solo para CA (donde el color preferido es el amarillo)*/
    df
      .groupBy($"State", $"Color")
      .sum("Count").as("Cantidad por estado y color")
      .filter($"State"=== "CA")
      .show()
  }
}
