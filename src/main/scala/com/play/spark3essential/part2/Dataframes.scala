package com.play.spark3essential.part2

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

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



}
