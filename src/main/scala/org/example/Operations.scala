package org.example

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Column, DataFrame}

case class Operations() {

  def fetchAverageGroupedBy(df: DataFrame,
                            column: Column,
                            avgColumn: Column,
                            as: String): DataFrame =
    df.groupBy(column)
      .agg(avg(avgColumn).as(as))
      .na.fill(0, Seq(as))

  def fetchGreaterOrEqual(df: DataFrame, column: Column, value: Double): DataFrame =
    df.filter(column >= value)
}
