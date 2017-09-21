package project2a

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import scalax.chart.module.Charting
import scalax.chart.api._


class Project2A {
  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Project2A")
    .getOrCreate()

  def joinData: Unit = {
	val tab = sparkSession.read.json("src/main/resources/project2a/dataOut.json")
        tab.show()
	tab.createOrReplaceTempView("TempTab")
        val tab2 = sparkSession.sql("SELECT name AS name2, id AS id2, city AS city2, company AS company2 FROM TempTab")
	val tab3 = tab.join(tab2, tab.col("name").equalTo(tab2.col("name2")) && tab.col("id").notEqual(tab2.col("id2")), "left")
   	tab3.show(100)
	
        tab.createOrReplaceTempView("Employees")
	val employeesTab = sparkSession.sql("SELECT name, city, company,(COUNT(name)) AS amount FROM Employees GROUP BY city,company,name ORDER BY amount DESC")
        employeesTab.show(100)

        val seq = employeesTab.collect().toSeq
        val chartTab = for (i <- seq) yield (i(0).toString + "; " + i(1).toString + "; " + i(2).toString, i(3).asInstanceOf[Long])

        val chart = PieChart(chartTab)
        chart.show()
        Thread.sleep(10000)
	  
  }
}
    
