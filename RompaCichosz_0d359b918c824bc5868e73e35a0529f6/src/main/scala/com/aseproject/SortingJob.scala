package com.aseproject
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
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
    val t0 = System.currentTimeMillis()
    println("Selection sort clock started ...")

    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count().toInt
    //println(splittedPairs.rdd.getNumPartitions)
    var minRecord = sparkSession.sql("SELECT * FROM source_table ORDER BY value").limit(1)
    var output_table = minRecord.select("id", "value")

    for(i <- 2 until (len)) {
      minRecord = sparkSession.sql("SELECT * FROM source_table ORDER BY value").limit(i)
      minRecord.createOrReplaceTempView("tmp")
      minRecord = sparkSession.sql("SELECT * FROM tmp WHERE value=(SELECT MAX(value) FROM tmp)")
      output_table = output_table.union(minRecord)
    }

    val outputFile = output_table.toJSON.collect()
    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "selectionSortOutput.json" ))

    for(l <- outputFile)
      {
        writer.println(l)
      }

    writer.close()
    val t1 = System.currentTimeMillis()
    println("Selection sort done. Execution time: " + (t1 - t0) + " ms")

    output_table.show()

  }

  def sparkPureSort: Unit = {
    val t0 = System.currentTimeMillis()
    println("Spark pure sort clock started ...")
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view ORDER BY value")

    val outputFile = splittedPairs.toJSON.collect()
    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "sparkPureSortOutput.json" ))

    for(l <- outputFile)
    {
      writer.println(l)
    }

    writer.close()

    val t1 = System.currentTimeMillis()
    println("Pure spark sort done. Execution time: " + (t1 - t0) + " ms")


  }
}