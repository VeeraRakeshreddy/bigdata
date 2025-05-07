package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.SparkConf
import scala.collection.immutable.List


object DataFrame {


	def main(args:Array[String]):Unit={

			val con = new SparkConf().setAppName("Mine").setMaster("local[*]")
					val sc = new SparkContext(con)
					sc.setLogLevel("ERROR")


					val spark = SparkSession.builder().getOrCreate()  
					import spark.implicits._


					def flatten(lst: List[Any]): List[Any] = lst flatMap {
					case i: Int     => List(i)
					case a: String     => List(a)
					case l: List[_] => flatten(l)
					
			}
			


			val data = List(1, 2, List(3, List(4,"r",5), 5, 6, 7))
			 
			val flattend = flatten(data)
			
			flattend.foreach(println)

			
			val data1 = "aaaabbbbcccc"
			
			val grouped = data1.toList.groupBy(identity).values.toList.map(x=>x.mkString)
			
			grouped.foreach(println)
			
			
			val str = "rakesh"
			val rev = str.reverse

					/*     val data = sc.textFile("file:///C:/data/txns.txt")

     val mapdata = data.map(x=>x.split(","))*/

					/* val structtype = StructType(Array(
         StructField("Code",StringType),
         StructField("Population",StringType),
         StructField("GDP",StringType),
         StructField("Capital",StringType),
         StructField("Country",StringType)))


         //=============================================================
      val orc = spark.read.format("orc").load("file:///C:/Users/veera/Downloads/sample1/sample1")
    orc.show()
         //==============================================================
      al rowrdd = mapdata.map(x=>Row(x(0),x(1),x(2),x(3),x(4)))
      rowrdd.foreach(println)
      val DataFrame = spark.createDataFrame(rowrdd,structtype)
      DataFrame.show()
      DataFrame.createOrReplaceTempView("MYVIEW")
      val fildata = spark.sql("select * from MYVIEW where Code='I'")
      fildata.show()
         //=========================TASK-1======================================

      val df = spark.read.format("csv").schema(structtype).load("file:///C:/data/txns.txt")
      df.show();

    //===========================TASK-2================================
    df.createOrReplaceTempView("Mydata")

    val Sql = spark.sql("select * from Mydata where Country ='IND'")

    Sql.show();*/

	}
}