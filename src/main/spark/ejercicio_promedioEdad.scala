object ejercicio_promedioEdad extends InitSpark {
  import org.apache.spark.sql.DataFrame
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df :  DataFrame = spark
                            .sparkContext
                            .parallelize(Seq(("Brooke", 20), ("Denny", 31), ("Jules", 30),("TD", 35), ("Brooke", 25)))
                            .toDF("nombre","edad")

    df.show()

    df
      .groupBy($"nombre")
      .avg("edad").as("Promedio edad")
      .show()
  }
}
