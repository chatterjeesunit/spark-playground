package com.play.spark3essential

import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession, functions}

object DataFrameOperations extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("DataFrame-Operations")
    .getOrCreate()

  val employeesSchema = StructType(
    Seq(
      StructField("Name", StringType, false),
      StructField("Gender", StringType, false),
      StructField("Joining_Date", DateType, false),
      StructField("Role", StringType, false),
      StructField("Salary", StringType, false),
      StructField("Location", StringType, false),
      StructField("Country", StringType, false)
    ))

  val employeesDf = sparkSession.read
    .options(Map(
      "header" -> "true",
      "dateFormat" -> "dd/MM/yyyy"
    ))
    .schema(employeesSchema)
    .csv("src/main/resources/data/employees.csv")

  employeesDf.show(5)



  // Create a new DF with only name and role of employees
  employeesDf
    .select("Name", "Role")
    .show(5)

  employeesDf
    .select(col("Name"), col("Role"))
    .show(5)

  employeesDf
    .select(expr("Name"), expr("Role"))
    .show(5)


  employeesDf
    .select(
      col("Name"),
      expr("Salary / 1000")
    )
    .show(5)


  employeesDf
    .select(
      col("Name"),
      expr("Salary / 1000") cast IntegerType as "Salary (K)"
    )
    .show(5)

  val employeeRole = functions.concat(
    expr("Name"),
    lit(" - "),
    expr("Role")
  )
  val yearOfJoining = functions.year(
    col("Joining_Date")
  )
  val salaryDisplayStr: Column = functions.concat(
    expr("Salary / 1000") cast IntegerType,
    lit("K")
  )

  // Employee-Role, Year of Joining and Salary (K)
  employeesDf
    .select(
      employeeRole as "Employee-Role",
      yearOfJoining as "Year of Joining",
      salaryDisplayStr as "Salary (K)"
    )
    .show(5, false)


  employeesDf
    .withColumn("Year of Joining", yearOfJoining)
    .withColumn("Salary (K)", salaryDisplayStr)
    .withColumnRenamed("Location", "City")
    .drop("Gender", "Joining_Date", "Salary")
    .show(5)


  employeesDf
    .select("Role")
    .distinct()
    .show()


  // We can put all conditions in one filter and use `and` function
  employeesDf
    .filter(
      expr("Location") === "Pune"
        and
        expr("Role") === "Developer"
        and
        expr("Salary / 1000") > 150
    )
    .show()

  // OR we can use a filter/where chain
  employeesDf
    .filter(expr("Location") === "Pune")
    .filter(expr("Role") === "Developer")
    .filter(expr("Salary / 1000") > 150)
    .show()

}
