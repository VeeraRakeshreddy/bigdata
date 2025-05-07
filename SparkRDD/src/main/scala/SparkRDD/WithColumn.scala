package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

object WithColumn {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df = spark.read.format("csv").option("header", "true").load("file:///C:/data/country_info.txt")

					df.show()
					df.persist()
					
					//withColumn() in deep bit
					val addcol = df
					.withColumn("Country_Code",expr("CASE WHEN Country_Code = 'I' THEN 'INDIA'  WHEN Country_Code = 'S' THEN 'USA' ELSE 'United Kingdom' end"))
					.withColumn("Population", expr("split(Population,' ')[0]"))
					.withColumnRenamed("Population", "COUNT")

					addcol.show()
					
					
				/*	//Aggregation 
					val aggre = addcol
					.groupBy("Country")
					.agg(
							sum("count")
							.alias("Total")
							).orderBy(col("Country"))

					aggre.show()*/
					
					
				// Task-1 add Extra column as Name and hard code it as "ZEYO"
					
					val extcol = addcol.withColumn("Name", expr(" 'Zeyo'"))
					extcol.show()
				//Task-2 
					
					val df1 = spark.read.format("csv").option("header", "true").load("file:///C:/data/join1.csv")
					val df2 = spark.read.format("csv").option("header", "true").load("file:///C:/data/join2.csv")
					
					df1.show()
					df2.show()
					
					val myjoin = df1.join(df2,Seq("ID"),"left_anti")
					val crossjoin = df1.crossJoin(df2).drop(df2("ID"))
					
					myjoin.show()
					crossjoin.show()
					

					df.unpersist()


	}



}