package com.play.spark3essential

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.util.Properties

object ReadingDataFrames extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Reading-Dataframes-demo")
    .getOrCreate()


  // Person Schema
  val personSchema = StructType(
    Seq(
      StructField("First Name", StringType, true),
      StructField("Last Name", StringType, true),
      StructField("Age", IntegerType, false),
      StructField("Gender", StringType, true),
      StructField("Country", StringType, true),
      StructField("Occupation", StringType, true)
    ))

  println("Reading person data in PERMISSIVE mode")
  // Default read mode - permissive
  // reading a json file which has 1 row which does not confirms to the schema
  val personDataFrame = sparkSession
    .read
    .format("json")
    .option("path", "src/main/resources/data/persons_data_malformed.json")
    .schema(personSchema)
    .load()

  // will return all 4 person records, with the `age` field set to `null` in the malformed record
  personDataFrame.show()


  println("Reading person data in DROPMALFORMED mode")
  // reading with mode - dropMalformed
  sparkSession
    .read
    .format("json")
    .options(Map(
      "path" -> "src/main/resources/data/persons_data_malformed.json",
      "mode" -> "dropMalformed"
    ))
    .schema(personSchema)
    .load()
    .show()


  println("Reading person data in FAILFAST mode")
  // reading with mode - failFast
  // will throw error
  // - org.apache.spark.SparkException: Malformed records are detected in record parsing. Parse Mode: FAILFAST.
  sparkSession
    .read
    .format("json")
    .options(Map(
      "path" -> "src/main/resources/data/persons_data_malformed.json",
      "mode" -> "failFast"
    ))
    .schema(personSchema)
    .load()
  //    .show() // uncommenting this line will throw error


  // READING DATE fields from json file.
  val personWithDateSchema = StructType(
    Seq(
      StructField("First Name", StringType, true),
      StructField("Last Name", StringType, true),
      StructField("Age", IntegerType, false),
      StructField("Gender", StringType, true),
      StructField("Country", StringType, true),
      StructField("Occupation", StringType, true),
      StructField("DateOfBirth", DateType, false)
    ))

  // We will read DateOfBirth field, which is format as "31 Dec 1999"
  sparkSession
    .read
    .format("json")
    .options(Map(
      "path" -> "src/main/resources/data/persons_data.json",
      "mode" -> "failFast",
      "dateFormat" -> "dd MMM yyyy"
    ))
    .schema(personWithDateSchema)
    .load()
    .show()

  //  // if we are using the date format with year as YYYY instead of yyyy,
  //  // then we will have to add this configuration in spark session to use legacy time parsing
  //  sparkSession.conf.set( "spark.sql.legacy.timeParserPolicy",  "LEGACY")
  //  sparkSession
  //    .read
  //    .format("json")
  //    .options(Map(
  //      "path"-> "src/main/resources/data/persons_data.json",
  //      "mode" -> "failFast",
  //      "dateFormat" -> "dd MMM YYYY"
  //    ))
  //    .schema(personWithDateSchema)
  //    .load()
  //    .show()


  sparkSession
    .read
    .options(Map(
      "mode" -> "failFast",
      "dateFormat" -> "dd MMM yyyy"
    ))
    .schema(personWithDateSchema)
    .json("src/main/resources/data/persons_data.json")
    .show()


  // Reading from CSV file
  val stockSchema = StructType(
    Seq(
      StructField("symbol", StringType, false),
      StructField("date", DateType, false),
      StructField("price", DoubleType, false)
    ))

  sparkSession
    .read
    .schema(stockSchema)
    .options(Map(
      "header" -> "true",
      "dateFormat" -> "MMM d yyyy",
      "sep" -> ","
    ))
    .csv("src/main/resources/data/stocks.csv")
    .show()


  // reading from a text file
  sparkSession
    .read
    .text("src/main/resources/data/sample_text.txt")
    .show(false)


  // reading from a postgres db
  val dbProperties: Properties = new Properties()
  dbProperties.setProperty("driver", "org.postgresql.Driver")
  dbProperties.setProperty("user", "pgadmin")
  dbProperties.setProperty("password", "pgadmin")

  sparkSession
    .read
    .jdbc(
      "jdbc:postgresql://localhost:5432/testdb",
      "employee_data",
      dbProperties
    )
    .show()

}
