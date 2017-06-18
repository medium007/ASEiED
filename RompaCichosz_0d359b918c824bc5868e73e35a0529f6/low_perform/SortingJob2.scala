package com.jwszol
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import java.io._

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
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count().toInt
    //println(splittedPairs.rdd.getNumPartitions)
    var minRecord = sparkSession.sql("SELECT * FROM source_table ORDER BY value").limit(1)
    var output_table = minRecord.select("id", "value")
    output_table.createOrReplaceTempView("output_table")

    for(i <- 2 until (len)) {
      minRecord = sparkSession.sql("SELECT * FROM source_table EXCEPT SELECT * FROM output_table")
      minRecord.createOrReplaceTempView("tmp")
      minRecord = sparkSession.sql("SELECT * FROM tmp WHERE value=(SELECT MAX(value) FROM tmp)")
      output_table = output_table.union(minRecord)
      output_table.createOrReplaceTempView("output_table")

    }

    output_table.show()
    val outputFile = output_table.toJSON.collect()
    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "selectionSortOutput.json" ))

    for(l <- outputFile)
      {
        writer.println(l)
      }

    writer.close()

  }
}