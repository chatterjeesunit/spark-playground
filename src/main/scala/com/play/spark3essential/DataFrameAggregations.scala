package com.play.spark3essential

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr, max, min, month, year}
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, StringType, StructField, StructType}

object DataFrameAggregations extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("DataFrame-Aggregations")
    .getOrCreate()

  val weatherSchema = StructType(
    Seq(
      StructField("city", StringType),
      StructField("date", DateType),
      StructField("precipitation", DecimalType(20,2)),
      StructField("temp_max", DecimalType(20,2)),
      StructField("temp_min", DecimalType(20,2)),
      StructField("wind", DecimalType(20,2)),
      StructField("weather", StringType)
    ))


  val weatherDf = sparkSession.read
    .options(Map(
      "header" -> "true",
      "dateFormat" -> "yyyy-MM-dd"
    ))
    .schema(weatherSchema)
    .csv("src/main/resources/data/weather.csv")

//  weatherDf.show()


  /*
    MIN MAX AVG
    Suppose we want to do this - find out the min/max/avg temperature in New York when it rained.
      - min temp = minimum of "temp_min"
      - max temp = maximum of "temp_max"
      - avg temp = average of "(temp_min + temp_max)/2"
   */

  val meanTemp = expr("(temp_min + temp_max)/2") cast DecimalType(20,2)
  val cityIsNY = col("city") === "New York"
  val weatherRained = col("weather") === "rain"

  // Individual aggregations using select
  weatherDf
    .withColumn("mean_temp", meanTemp)
    .filter(cityIsNY && weatherRained)
    .select(
      min("temp_min") as "MinTemp",   //refer to column directly using column name
      max(col("temp_max")) as "MaxTemp", //refer to colum using col
      avg("mean_temp") cast DecimalType(20,2) as "AvgMeanTemp"
    )
    .show()

  // Individual aggregations using "agg" function
  weatherDf
    .withColumn("mean_temp", meanTemp)
    .filter(cityIsNY && weatherRained)
    .agg(
      min("temp_min") as "MinTemp",
      max("temp_max") as "MaxTemp",
      avg("mean_temp") cast DecimalType(20,2) as "AvgMeanTemp"
    )
    .show()


  /*
    MIN MAX AVG - GROUP BY CITY and WEATHER
      - min temp = minimum of "temp_min"
      - max temp = maximum of "temp_max"
      - avg temp = average of "(temp_min + temp_max)/2"
   */


  // Group By
  weatherDf
    .withColumn("mean_temp", meanTemp)
    .groupBy(
      col("city"),
      col("weather")
    )
    .agg(
      min("temp_min") as "MinTemp",
      max("temp_max") as "MaxTemp",
      avg("mean_temp") cast DecimalType(20,2) as "AvgMeanTemp"
    )
    .orderBy("city", "weather")
    .show()



  // For days it snowed,
  // get max/min temp by year and month and city
  weatherDf
    .filter(col("weather") === "snow")  // filter rows for when it snowed
    .withColumn("year", year(col("date")))  // extract year from date
    .withColumn("month", month(col("date")))  // extract month from date
    .groupBy(     // group by year, month and city
      col("year"),
      col("month"),
      col("city")
    )
    .agg(         // perform aggregations
      min("temp_min") as "MinTemp",
      max("temp_max") as "MaxTemp"
    )
    .orderBy("year", "month", "city")
    .show()
}
