package com.play.spark3essential

import org.apache.spark.sql.functions.{avg, col, collect_set, concat_ws}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameJoinsAndAgg extends App {

  val sparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("DataFrame-Joins-Aggs")
      .getOrCreate()

  def readCSV(path: String): DataFrame = sparkSession
    .read
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true"))
    .csv(path)

  val moviesDf = readCSV("src/main/resources/data/movies-dataset-small/movies.csv")
  val ratingsDf = readCSV("src/main/resources/data/movies-dataset-small/ratings.csv")
  val tagsDf = readCSV("src/main/resources/data/movies-dataset-small/tags.csv")

//  moviesDf.show(5)
//  moviesDf.printSchema()
//  ratingsDf.show(5)
//  ratingsDf.printSchema()
//  tagsDF.show(5)
//  tagsDF.printSchema()

  println(s"Total # of movies = ${moviesDf.count()}")


  //rename common column name in ratingsDf
  val ratingsDfWithColsRenamed =
    ratingsDf
      .withColumnRenamed("movieId", "ratings_movieId")
      .withColumnRenamed("userId", "ratings_userId")
      .withColumnRenamed("timestamp", "ratings_ts")


  val tagsDfWithColsRenamed =
    tagsDf
      .withColumnRenamed("movieId", "tags_movieId")
      .withColumnRenamed("userId", "tags_userId")
      .withColumnRenamed("timestamp", "tags_ts")

  // get movies along with ratings and tags - inner join
  val moviesWithRatingsAndTags =
    moviesDf
      .join(
        ratingsDfWithColsRenamed, // join with dataframe
        col("movieId") === col("ratings_movieId"),  // join condition
        "inner" // join type
      )
      .join(
        tagsDfWithColsRenamed,
        col("movieId") === col("tags_movieId"),
        "inner"
      )
      .drop(
          "ratings_movieId", "tags_movieId",
          "ratings_ts", "tags_ts")
      .groupBy("movieId", "title", "genres")
      .agg(
        avg("rating") cast DecimalType(20,2) as "AverageRating",
        concat_ws(",", collect_set("tag")) as "AllTags"
      )
      .orderBy("movieId")

  moviesWithRatingsAndTags.show(10, false)

}
