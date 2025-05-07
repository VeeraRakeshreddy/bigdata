package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object DSL {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df =  spark.read.format("csv").option("header","true").load("file:///C:/data/country_info.txt") 

					df.show()
					df.persist()

					//Show only country column from the table

					val selectdata = df.select("Country")
					selectdata.show()

					//show the records on where country = IND and Capital = Delhi
					val filterdata = df.filter(col("Country")==="IND" && $"Capital"==="Delhi")
					filterdata.show()

					//filter on single column with multi value
					val multival = df.filter(col("Country").isin("IND","US"))
					multival.show(80)

					//to show a text ignoring prefix and suffix of %hra%
					val likdata = multival.filter(col("Capital").like("%hra%"))
					likdata.show()
					//mixed filter
					val multifilter = df.filter(col("Country")==="IND" && col("Capital").like("%hra%"))
					multifilter.show()

					//Failure case
					/*val Fextcolm = df.select("Country_Code","Population","GDP","Capital","Country","case when Country_Code ='I' then 'INDIA' else if  Country_Code ='S' then 'US' else 'UK' end as Full_Country_Code" )
					Fextcolm.show()*/

					//create an Conditional expression and save that data in a new cloumn
					val Sextcolm = df.selectExpr("Country_Code","Population","GDP","Capital","Country","case when Country_Code ='I' then 'INDIA' when Country_Code ='S' then 'US' else 'UK' end as Full_Country_Code" )
					Sextcolm.show(150)


					//create an Conditional expression and save that data in a new cloumn without adding all columns in the selectExpr
					val withcolumn = df.withColumn("MyCountryColumn", expr("CASE WHEN Country_Code ='I' THEN 'INDIA' WHEN Country_Code ='S' THEN 'US' ELSE 'UK' END"))
					withcolumn.show()

					//Task-1
					val splitpopulation = df.selectExpr("Country_Code","split(Population,' ')[0] as Population","GDP","Capital","Country")
					splitpopulation.show()

					//Task-2
					val withsplit = df.withColumn("Population", expr("split(Population,' ')[0]"))
					withsplit.show()
					df.unpersist()
	}

}