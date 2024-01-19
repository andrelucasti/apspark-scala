package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}

object UserReviewsDataFrame extends App {

  private val spark: SparkSession = SparkSession.builder().appName("DataFrames").config("spark.master", "local")
    .getOrCreate()

//App,Translated_Review,Sentiment,Sentiment_Polarity,Sentiment_Subjectivity

  val schema = StructType(
    Array(
      StructField("App", StringType, true),
      StructField("Translated_Review", StringType, true),
      StructField("Sentiment", StringType, true),
      StructField("Sentiment_Polarity", DoubleType, true),
      StructField("Sentiment_Subjectivity", DoubleType, true)
    )
  )



  private val df = spark.read.format("csv")
    .options(
      Map(
        "header" -> "true",
        "inferSchema" -> "true",
        "sep" -> ","
      )
    )
    .schema(schema)
    .load(UtilsResource.googlePlayStoreUserReviewsCsv)

  // Average of the column Sentiment_Polarity grouped by App name
  val df1 = df.groupBy(col("App"))
    .agg(
      avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"),
    )
    .na.fill(0, Seq("Average_Sentiment_Polarity"))

  // Average of the column Sentiment_Subjectivity grouped by App name

  val df1_1 = Operations()
    .fetchAverageGroupedBy(df,
      col("App"),
      col("Sentiment_Subjectivity"), "Average_Sentiment_Subjectivity")

  df1_1.show()
}
