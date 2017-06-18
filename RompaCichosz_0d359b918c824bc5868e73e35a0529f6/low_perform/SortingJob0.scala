package com.jwszol
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Column


class SortingJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val path = "./src/main/resources/dataMay-31-2017.json"
  val MapPartRDD = sparkSession.sparkContext.wholeTextFiles(path).values
  val rawData = sparkSession.read.json(MapPartRDD)
  val extractedPairs = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
  extractedPairs.createOrReplaceTempView("pairs_view")

  def selectionSort: Unit = {
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    val len = splittedPairs.count()
    splittedPairs.createOrReplaceTempView("source_table")

    val minRecord  = sparkSession.sql("SELECT * FROM source_table WHERE value=(SELECT MIN(value) FROM source_table)")
    minRecord.createOrReplaceTempView("output_table")
    minRecord.createOrReplaceTempView("minRecord")
    sparkSession.sql("SELECT * FROM output_table UNION SELECT * FROM minRecord").createOrReplaceTempView("output_table")
    sparkSession.sql("SELECT * FROM output_table").show()

    for(i <- 0 to 4)
      {
        val diff = sparkSession.sql("SELECT * FROM source_table EXCEPT SELECT * FROM minRecord")
        diff.createOrReplaceTempView("source_table")
        val minRecord = sparkSession.sql("SELECT * FROM source_table WHERE value=(SELECT MIN(value) FROM source_table)")
        minRecord.createOrReplaceTempView("minRecord")
        sparkSession.sql("SELECT * FROM output_table UNION SELECT * FROM minRecord").createOrReplaceTempView("output_table")

      }
    sparkSession.sql("SELECT * FROM output_table").show()

  }


}

