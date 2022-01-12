package com.play.spark3essential.part2

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object WritingDataFrames extends App {

  // create a spark session
  val sparkSession: SparkSession =
    SparkSession
      .builder()
      .master("local") //Run locally
      .appName("SparkSessionDemo")
      .getOrCreate() // get existing session, and if not available, then create one


  // creating data frames manually - just provide a seq of tuples
  val personData:  Seq[(String, String, Int, String, String, String)] =
    Seq(
      ("John", "Doe", 35, "Male", "USA", "Product Manager"),
      ("Pedro", "Iglesia", 25, "Male", "Spain", "Software Developer"),
      ("Ben", "Davis", 53, "Male", "Netherlands", "Architect"),
      ("Sofia", "Tanner", 28, "Female", "Canada", "Doctor")
    )

  import sparkSession.implicits._

  // Writing data frames to a JSON FILE
  val personDF: DataFrame = personData
    .map(x => (x._1, x._2, x._5, x._6))
    .toDF(
      "First Name", "Last Name", "Country", "Occupation")

  personDF
    .write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/output/persons.json")
    .save()





  // writing to a csv file
  personDF
    .write
    .mode(SaveMode.Overwrite)
    .options(Map(
      "header" -> "true",
      "sep" -> ";"
    ))
    .csv("src/main/resources/data/output/persons.csv")




  // writing to a parquet
  personDF
    .select(
      col("First Name") alias("FName"),
      col("Last Name") alias("LName"),
      col("Country"),
      col("Occupation")
    )
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/output/persons.parquet")



  // Writing to a database table
  val dbProperties: Properties = new Properties()
  dbProperties.setProperty("driver", "org.postgresql.Driver")
  dbProperties.setProperty("user", "pgadmin")
  dbProperties.setProperty("password", "pgadmin")

  personDF
    .write
    .mode(SaveMode.Append)
    .jdbc(
      "jdbc:postgresql://localhost:5432/testdb",
      "dummy_person_data",
      dbProperties
    )



}
