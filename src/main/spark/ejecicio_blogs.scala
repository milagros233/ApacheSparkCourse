object ejecicio_blogs extends InitSpark {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val path= "C:\\workspace\\scala\\Apache_Spark _Course\\src\\main\\data_origen\\blogs.json"
    val df= spark
      .read
      .schema("Id Int, First String, Last String, Url String, Published String, Hits Int, Campaigns Array<String>")
      .json(path)

    /*SE CREA UNA COLUMNA CONDICIONAL*/
    df.withColumn("Big Hitters",$"Hits"> 10000).show()

    /*SE CREAU UNA COLUMNA QUE CONCATENA Y SOLO SELECCIONAR UN CAMPO*/
    df.withColumn("AutorId",concat($"First",$"Last",$"Id")).select("AutorId").show()

    /*ORDENAR POR COLUMNA "ID" EN ORDEN DESCENDENTE*/
    df
      .sort($"Id".desc)
      .show()


  }
}
