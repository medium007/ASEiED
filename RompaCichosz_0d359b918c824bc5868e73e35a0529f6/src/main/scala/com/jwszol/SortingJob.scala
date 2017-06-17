package com.jwszol
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

class SortingJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val path = "./src/main/resources/dataMay-31-2017.json"
  val MapPartRDD = sparkSession.sparkContext.wholeTextFiles(path).values
  val rawData = sparkSession.read.json(MapPartRDD)
  var WrappedArray = rawData.head().getList(1)
  var pairs  = WrappedArray.toArray   //(id, value)

  def selectionSort: Unit = {

    println("test")
    rawData.show()

  }
}

