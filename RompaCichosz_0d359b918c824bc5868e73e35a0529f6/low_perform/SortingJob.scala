package com.aseproject
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

class SortingJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .config("spark.default.parallelism", 2)
    .getOrCreate()

  val path = "./src/main/resources/dataMay-31-2017.json"
  val MapPartRDD = sparkSession.sparkContext.wholeTextFiles(path).values
  val rawData = sparkSession.read.json(MapPartRDD)
  val extractedPairs = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
  extractedPairs.createOrReplaceTempView("pairs_view")

  def selectionSort: Unit = {
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view").cache()
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count()
    println(splittedPairs.rdd.getNumPartitions)

    var minRecord = sparkSession.sql("SELECT * FROM source_table WHERE value=(SELECT MIN(value) FROM source_table)").cache()
    //minRecord.show()
    //println(minRecord.rdd.getNumPartitions)
    var output_table = minRecord.select("id", "value").cache()
    //println(output_table.rdd.getNumPartitions)

    for(i <- 0 to 2) {
      println(i)
      splittedPairs = splittedPairs.except(minRecord)
      splittedPairs.createOrReplaceTempView("source_table")
      //println(splittedPairs.rdd.getNumPartitions)
      minRecord = sparkSession.sql("SELECT * FROM source_table WHERE value=(SELECT MIN(value) FROM source_table)")
      //println(minRecord.rdd.getNumPartitions)

      output_table = output_table.union(minRecord)
      println(minRecord.rdd.getNumPartitions)
      println(output_table.rdd.getNumPartitions)

    }
    output_table.show()

  }
}