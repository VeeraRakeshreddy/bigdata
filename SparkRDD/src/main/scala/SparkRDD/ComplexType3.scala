package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source.fromURL


object ComplexType3 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					val jsonurl = fromURL("https://randomuser.me/api").mkString
					val urlrdd = sc.parallelize(List(jsonurl))
					val df = spark.read.json(urlrdd)
					
					df.printSchema()
					
					
					val explodedata = df.withColumn("results",explode(col("results")))
					
					explodedata.printSchema()
					
					val structdata = explodedata.select(
					    
					          "info.*",
					          "results.cell",
					          "results.dob.age",
					          "results.dob.date",
					          "results.email",
					          "results.gender",
					          "results.id.name",
					          "results.id.value",
					          "results.location.city",
					          "results.location.coordinates.latitude",
					          "results.location.coordinates.longitude",
					          "results.location.country",
					          "results.location.postcode",
					          "results.location.state",
					          "results.location.street.name",
					          "results.location.street.number",
					          "results.location.timezone.description",
					          "results.location.timezone.offset",
					          "results.login.*",
					          "results.name.*",
					          "results.nat",
					          "results.phone",
					          "results.picture.*",
					          "results.registered.*"
					         	
					)
					
					structdata.printSchema()
					structdata.show()
					
					
					

				/*	val rawjson = spark.read.format("json").option("multiline","true").load("file:///C:/data/donut_data.json")

					println("=============RAW DATA=====================")
					rawjson.show()
					rawjson.printSchema()*/

					/* val flatdata = rawjson.selectExpr(
              "id",
              "image.height as image_height",
              "image.url as image_url",
              "image.width as image_width",
              "name",
              "type", 
              "thumbnail.height as thumbnail_height",
              "thumbnail.url as thumbnail_url",
              "thumbnail.width as thumbnail_width"
                 )

         flatdata.show()
         flatdata.printSchema()*/

					//any method we can follow 

					/*val withcol = rawjson
      .withColumn(
      "image_height",expr("image.height") )
      .withColumn(
      "image_url",expr("image.url") )
      .withColumn(
      "image_width",expr("image.width") ).drop("image")
      .withColumn(
      "thumbnail_height",expr("thumbnail.height") )
      .withColumn(
      "thumbnail_url",expr("thumbnail.url") )
      .withColumn(
      "thumbnail_width",expr("thumbnail.width") ).drop("thumbnail")

      withcol.show()
      withcol.printSchema()*/



			/*		val complexarray = spark.read.format("json").option("multiline","true").load("file:///C:/data/ComplexArray.json")

					complexarray.show()
					complexarray.printSchema()

					val explodedata = complexarray
					.withColumn("Students",explode(col("Students")))
					.withColumn("TemporaryAddress_new", expr("Address.TemporaryAddress") )
					.withColumn("PermanentAddress_new", expr("Address.permanentAddress") ).drop(col("Address"))

					explodedata.show()
					explodedata.printSchema()*/

					/*val structflat = explodedata.select(
							"Address.*",
							"Students",
							"name",
							"age"
							)

					structflat.printSchema()*/

	}

}