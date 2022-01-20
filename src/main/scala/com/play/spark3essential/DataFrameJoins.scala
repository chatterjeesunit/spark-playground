package com.play.spark3essential

import com.play.spark3essential.Dataframes.sparkSession
import org.apache.spark.sql.functions.{avg, col, collect_set, concat_ws}
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameJoins extends App {

  val sparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("DataFrame-Joins")
      .getOrCreate()


  import sparkSession.implicits._
  val employeesDF = Seq (
    ("E001", "John", "D004"),
    ("E002", "Julie", "D002"),
    ("E003", "Ben", "D003"),
    ("E004", "Samantha", "D001"),
  ).toDF("EmpId", "EmployeeName", "DeptId")

  val deptDF = Seq(
    ("D001", "Sales"),
    ("D002", "Engineering"),
    ("D003", "HR & Finance"),
    ("D005", "IT"),
  ).toDF("DeptId", "DeptName")


  employeesDF.show()
  deptDF.show()

  //Inner Join
  employeesDF
    .withColumnRenamed("DeptId", "Emp_DeptId")
    .join(
      deptDF,   // join with DF
      col("Emp_DeptId") === col("DeptId"), //join condition
      "inner" // join type
    )
    .show()


  //Left Outer Join
  employeesDF
    .withColumnRenamed("DeptId", "Emp_DeptId")
    .join(
      deptDF,   // join with DF
      col("Emp_DeptId") === col("DeptId"), //join condition
      "left_outer" // join type
    )
    .show()


  //Right Outer Join
  employeesDF
    .withColumnRenamed("DeptId", "Emp_DeptId")
    .join(
      deptDF,   // join with DF
      col("Emp_DeptId") === col("DeptId"), //join condition
      "right_outer" // join type
    )
    .show()


  //Full Outer Join
  employeesDF
    .withColumnRenamed("DeptId", "Emp_DeptId")
    .join(
      deptDF,   // join with DF
      col("Emp_DeptId") === col("DeptId"), //join condition
      "outer" // join type
    )
    .show()


  //Left Anti Join
  employeesDF
    .withColumnRenamed("DeptId", "Emp_DeptId")
    .join(
      deptDF,   // join with DF
      col("Emp_DeptId") === col("DeptId"), //join condition
      "left_anti" // join type
    )
    .show()


  //Left Semi Join
  employeesDF
    .withColumnRenamed("DeptId", "Emp_DeptId")
    .join(
      deptDF,   // join with DF
      col("Emp_DeptId") === col("DeptId"), //join condition
      "left_semi" // join type
    )
    .show()

}
