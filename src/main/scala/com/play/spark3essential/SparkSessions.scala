package com.play.spark3essential

import org.apache.spark.sql.SparkSession

object SparkSessions extends App {

  // create a spark session
  val sparkSession1: SparkSession =
    SparkSession
      .builder()
      .master("local") //Run locally
      .appName("SparkSessionDemo")
      .getOrCreate() // get existing session, and if not available, then create one

  //Run locally with 3 nodes
  val sparkSession2 = SparkSession.builder().master("local[3]").appName("demo2").getOrCreate()

  // Run locally, but configure master node via a CONFIG
  val sparkSession3 = SparkSession.builder().config("spark.master", "master").appName("demo3").getOrCreate()

  val sparkSession4 = SparkSession.builder().master("spark://master:7077").appName("demo4").getOrCreate()

  // We are trying to create a new session now
  val sparkSession5 = sparkSession1.newSession()

  // All sessions 1-4 are exactly the same, since we call `getOrCreate` and it returns same session if it exists
  // However Session 5 will be a new session, but will share the same spark context
  println(s"Spark Session = $sparkSession1 , Spark Context = ${sparkSession1.sparkContext}")
  println(s"Spark Session = $sparkSession2 , Spark Context = ${sparkSession2.sparkContext}")
  println(s"Spark Session = $sparkSession3 , Spark Context = ${sparkSession3.sparkContext}")
  println(s"Spark Session = $sparkSession4 , Spark Context = ${sparkSession4.sparkContext}")
  println(s"Spark Session = $sparkSession5 , Spark Context = ${sparkSession5.sparkContext}")

  // Get all conf
  val allConf: Map[String, String] = sparkSession1.conf.getAll
  allConf.foreach(println)

  // Get a single configuration
  val crossJoinEnabled: String = sparkSession1.conf.get("spark.sql.crossJoin.enabled")
  val masterConf: String = sparkSession1.conf.get("spark.master")

  println(crossJoinEnabled)
  println(masterConf)

  // Set a configuration
  sparkSession1.conf.set("spark.sql.crossJoin.enabled", false)
  println(sparkSession1.conf.get("spark.sql.crossJoin.enabled"))


  // To close a session we can call either `stop` or `close`. Both are same.
  sparkSession1.close()

  // Checking session state and context now.
  println(s"Session 1 ContextStopped? = ${sparkSession1.sparkContext.isStopped}")
  println(s"Session 2 ContextStopped? = ${sparkSession2.sparkContext.isStopped}")
  println(s"Session 3 ContextStopped? = ${sparkSession3.sparkContext.isStopped}")
  println(s"Session 4 ContextStopped? = ${sparkSession4.sparkContext.isStopped}")
  println(s"Session 5 ContextStopped? = ${sparkSession5.sparkContext.isStopped}")

}
