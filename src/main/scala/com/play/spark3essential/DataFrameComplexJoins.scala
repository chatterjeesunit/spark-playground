package com.play.spark3essential

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, coalesce, col, collect_set, concat_ws, count, explode}

import scala.reflect.internal.ClassfileConstants.ifnull

object DataFrameComplexJoins extends App {

  val sparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("DataFrame-ComplexJoins")
      .getOrCreate()


  import sparkSession.implicits._
  val employeesDF = Seq (
    ("E001", "John", List("S004", "S001")),
    ("E002", "Julie", List("S002")),
    ("E003", "Ben", List("S004", "S001", "S002")),
    ("E004", "Samantha", List("S001", "S002")),
  ).toDF("EmpId", "EmployeeName", "Emp_Skills")

  val skillsDf = Seq(
    ("S001", "Java"),
    ("S002", "Scala"),
    ("S003", "Spark"),
    ("S004", "Golang"),
  ).toDF("SkillId", "SkillName")


  employeesDF.show()
  skillsDf.show()

  //  Use Case 1: We may want to show all employees and their skills
  employeesDF
    .withColumn("Emp_Skills_Exploded", explode(col("Emp_Skills")))
    .drop("Emp_Skills")
    .join(
      skillsDf,
      col("Emp_Skills_Exploded") === col("SkillId"),
      "inner"
    )
    .groupBy( col("EmpId"), col("EmployeeName"))
    .agg(
      collect_set( col("SkillName")) as "EmployeeSkills"
    )
    .withColumn( "Employee_Skills", concat_ws(",", col("EmployeeSkills"))
    )
    .drop("EmployeeSkills")
    .orderBy(col("EmpId"))
    .show(false)



  // Use Case 2: Find all employees who have skill as "Golang"
  employeesDF
    .withColumn("Emp_Skills_Exploded", explode(col("Emp_Skills")))
    .join(
      skillsDf.filter(col("SkillName") === "Golang"),
      col("Emp_Skills_Exploded") === col("SkillId"),
      "left_semi"
    )
    .drop("Emp_Skills_Exploded")
    .distinct()
    .orderBy(col("EmpId"))
    .show(false)


  // Use Case 3: show skills and the count of employees who have those skills

  val employeeDFWithSkillCounts =
    employeesDF
      .withColumn("Emp_Skills_Exploded", explode(col("Emp_Skills")))
      .groupBy("Emp_Skills_Exploded")
      .agg( count("*") as "Count")

  skillsDf
    .join(
      employeeDFWithSkillCounts,
      col("SkillId") === col("Emp_Skills_Exploded"),
      "left_outer"
    )
    .drop("Emp_Skills_Exploded")
    .na
    .fill(0, Array("Count"))
    .show()

}
