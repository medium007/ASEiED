package com.jwszol
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col


class SortingJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  def selectionSort: Unit = {

    println("test")


  }
}

