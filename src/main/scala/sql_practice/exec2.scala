package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exec2 {
  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._


/////// Question 2

    val sample07 = spark.read
        .option("delimiter", "\t")
        .csv("data/input/sample_07")
        .select($"_c0".as("code"),
          $"_c1".as("description"),
          $"_c2".as("total_emp_07"),
          $"_c3".as("salary_07"))

    sample07.show



    val sample08 = spark.read
      .option("delimiter", "\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"),
        $"_c1".as("description_08"),
        $"_c2".as("total_emp_08"),
        $"_c3".as("salary_08"))

    sample08.show

    sample07.orderBy($"salary_07".desc)
            .filter($"salary_07">100000)
            .show

    sample08.orderBy($"salary_08".desc)
            .filter($"salary_08">100000)
            .show

///////Question 2 % salaries

    val increased07 = sample07.orderBy($"salary_07".desc)
    val increased08 = sample08.orderBy($"salary_08".desc)

    increased07.join(increased08, Seq("code"))
               .filter($"salary_08">$"salary_07")
               .show

    val joinD = increased07.join(increased08, Seq("code"))

    val percentageS = joinD.select($"description",
                                    ($"salary_08"- $"salary_07").as("difference"),
                                    round(((($"salary_08"- $"salary_07")* 100)/$"salary_07"),2).as("Percentage")).show

    ///////Question 3 % job lost

    val percentageE = joinD.select($"description",
                                    ($"total_emp_08"- $"total_emp_07").as("difference"),
                                    round(((($"total_emp_08"- $"total_emp_07")* 100)/$"salary_07"),2).as("Percentage"))

    val filterPourcentage = percentageE.filter($"Percentage"< 0)
                                       .orderBy($"Percentage".desc)
                                       .show

  }
}