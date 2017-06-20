package com.jwszol

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col


/**
  * Created by jwszol on 11/06/17.
  */
class JoinJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  def joinData: Unit = {

    val users = sparkSession.read.option("delimiter", "|").option("header", "true").csv("./src/main/resources/users.txt")
    val transactions = sparkSession.read.option("delimiter", "|").option("header", "true").csv("./src/main/resources/transactions.txt")

    users.printSchema()
    transactions.printSchema()

    val tempdfUsers = users.select(col("email"), col("id").plus(1).name("new_id"))
    tempdfUsers.createOrReplaceTempView("users_view")

    sparkSession.sql("select * from users_view").show()
    val joinDs = transactions.join(users, transactions.col("user_id").equalTo(users.col("id")),"leftouter")
    joinDs.show()
  }

}

