package org.example

import org.apache.spark.sql.SparkSession

object DataFrames extends App{
  private val spark: SparkSession = SparkSession.builder().appName("DataFrames").config("spark.master", "local")
    .getOrCreate()

  val df = spark.read.format("csv")
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .load("src/main/resources/data/googleplaystore.csv")

  df.show()
  df.printSchema()
}
