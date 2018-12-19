package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exec3 {
  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show


/////// Question 1

    toursDF
      .select($"tourLength".as("Unique level"))
      .where($"tourLength" === 1)
      .agg(sum("Unique level"))
      .show

////// Question 2

    toursDF
      .select($"tourPrice")
      .agg(min("tourPrice").as("MinimumPrice"),
           max("tourPrice").as("MaximumPrice"),
           avg("tourPrice").as("AveragePrice"))
      .show




////// Question 3
    toursDF
      .select($"tourPrice",$"tourDifficulty")
      .groupBy( $"tourDifficulty")
      .agg(min("tourPrice").as("MinimumPrice"),
           max("tourPrice").as("MaximumPrice"),
           round(avg("tourPrice"),3).as("AveragePrice"))
      .show
////// Question 4
    toursDF
      .select($"tourPrice",$"tourDifficulty", $"tourLength")
      .groupBy( $"tourDifficulty")
      .agg(min("tourPrice").as("MinimumPrice"),
        max("tourPrice").as("MaximumPrice"),
        round(avg("tourPrice"),3).as("AveragePrice"),
        min("tourLength").as("MinimumLength"),
        max("tourLength").as("MaximumLength"),
        round(avg("tourLength"),3).as("AverageLength"))
      .show

////// Question 5
    toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .orderBy($"count".desc)
      .show(10)

////// Question 6

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)
////// Question 7
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty",$"tourPrice")
      .groupBy($"col", $"tourDifficulty")
      .agg(min("tourPrice").as("MinimumPrice"),
        max("tourPrice").as("MaximumPrice"),
        round(avg("tourPrice"),3).as("AveragePrice"))
      .orderBy($"AveragePrice".desc)
      .show

}
}
