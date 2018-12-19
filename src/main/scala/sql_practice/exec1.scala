
package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exec1 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    ///////Question 1

    val demoJson = spark.read
      .json("data/input/demographie_par_commune.json")
    demoJson.show



    demoJson.agg(sum("Population").as("Population of France")).show

///////Question 2

    demoJson.groupBy("Departement")
            .agg(sum("Population").as("total_Pop"))
            .orderBy($"total_Pop".desc).show

//////Question 3

    val topPopulation = demoJson.groupBy("Departement")
                                .agg(sum("Population").as("total_Pop"))
                                .orderBy($"total_Pop".desc)
    val departmentDF = spark.read.csv("data/input/departements.txt")
                                .select($"_c0".as("name"),
                                $"_c1".as("Departement"))



     departmentDF.join(topPopulation, Seq("Departement")).show



  }
}
