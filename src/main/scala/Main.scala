import spark_helpers.SessionBuilder

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.buildSession()

    val sparkVersion = spark.version
    println(s"Spark Version: $sparkVersion")

    //sql_practice.examples.exec1()
    //sql_practice.exec1.exec1()
//    sql_practice.exec2.exec2()
    sql_practice.exec3.exec3()
  }
}
