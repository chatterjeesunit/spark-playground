package com.play.spark3essential.part2

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Dataframes extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("Dataframes-demo")
    .master("local")
    .getOrCreate()


  // read data from a json file and infer schema structure (not recommened for prod)
  val dataFrame: DataFrame = sparkSession
    .read     // creates a DataFrameReader
    .option("inferSchema", true)  // provide options for the reader (
    .format("json")       // specify the format as JSON file
    .load("src/main/resources/data/cars.json")  // loads data from this path


  dataFrame.show() // shows top 20 records

  // show top 5 records
  dataFrame.show(5)
  // show 20 records, but dont truncate the long strings
  dataFrame.show(false)


  // we can also perform operations on DataFrame
  val cars =
    dataFrame
      .take(5)
      .foreach(println)


  // show the schema structure
  dataFrame.printSchema()

  val schema: StructType= dataFrame.schema
  println(schema)



  // Create own schema and use it to read dataframe
  // We are now reading only few fields per row ( and not all)
  val customSchema: StructType =
    StructType(
      Seq(
        StructField("Name",  StringType),
        StructField("Miles_per_Gallon",DoubleType),
        StructField("Displacement",DoubleType),
        StructField("Horsepower",LongType),
        StructField("Acceleration",DoubleType),
        StructField("Origin",StringType))
    )

  // We are now providing the schema
  // Also we provided path in `option` instead of `load` method
  val anotherDf =
    sparkSession
      .read
      .format("json")
      .option("path", "src/main/resources/data/cars.json")
      .schema(customSchema)
      .load()

  anotherDf.show(5)



  // creating data frames manually - just provide a seq of tuples
  val personData:  Seq[(String, String, Int, String, String, String)] =
    Seq(
      ("John", "Doe", 35, "Male", "USA", "Product Manager"),
      ("Pedro", "Iglesia", 25, "Male", "Spain", "Software Developer"),
      ("Ben", "Davis", 53, "Male", "Netherlands", "Architect"),
      ("Sofia", "Tanner", 28, "Female", "Canada", "Doctor")
    )

  val manualDF: DataFrame = sparkSession.createDataFrame(personData)

  manualDF.show()
  manualDF.printSchema()



  import sparkSession.implicits._

  val anotherManualDF = personData
    .toDF(
      "First Name", "Last Name", "Age",
      "Gender", "Country", "Occupation")

  anotherManualDF.show()
  anotherManualDF.printSchema()
  println(anotherManualDF.schema)


  // Person Schema
  val personSchema = StructType(
    Seq(
      StructField("First Name",StringType,true),
      StructField("Last Name",StringType,true),
      StructField("Age",IntegerType,false),
      StructField("Gender",StringType,true),
      StructField("Country",StringType,true),
      StructField("Occupation",StringType,true)
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
      "path"-> "src/main/resources/data/persons_data_malformed.json",
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
      "path"-> "src/main/resources/data/persons_data_malformed.json",
      "mode" -> "failFast"
    ))
    .schema(personSchema)
    .load()
//    .show() // uncommenting this line will throw error



  // Writing data frames
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


}
