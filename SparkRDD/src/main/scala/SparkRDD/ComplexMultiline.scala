package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object ComplexMultiline {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    //Complex data (Important)
    
    //Type-1 Multiline true
   /* val compjson = spark.read.format("json").option("multiline","true").load("file:///C:/data/compjson.json")
    
   compjson.show()
   compjson.printSchema()
   
   //Type 2 Struct complex datatype
   val structdata = compjson.select(
       "name",
       "Age",
       "Address",
       "Address.permanentAddress",
       "Address.TemporaryAddress"
   )
   
   structdata.show()*/
    
    //Task-1 
    val data = spark.read.format("json").option("multiline","true").load("file:///C:/data/compjson.json")
    
    val flatdata = data.select(
        "name",
        "age",
        "Address.*"
        )
    
    flatdata.show()
    flatdata.printSchema()
    //Task-2
    val complexdata = flatdata.select(

              col("name"),
              col("age"),
              struct(
                  col("permanentAddress"),
                  col("TemporaryAddress")
                  ).alias("Address")              
    
    )
    
    complexdata.show()
    complexdata.printSchema()
   
  }
  
}