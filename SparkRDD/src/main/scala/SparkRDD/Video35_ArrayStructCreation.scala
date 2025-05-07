package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Video35_ArrayStructCreation {
  
  
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val rawdata = spark.read.format("json").option("multiline","true").load("file:///C:/data/ComplexArrayStructjson.json")
    
    rawdata.printSchema()
    rawdata.show()
    
    val flatdata = rawdata.withColumn("Pets",explode(col("Pets")))
                           .withColumn("Students",explode(col("Students")))
                           .withColumn("Brand",explode(col("Students.user.Mobiles")))
                           .select(
                               
                               col("Address.*"),
                               col("Mobile"),
                               col("Name").alias("Student Name"),
                               col("status"),
                               col("Pets"),
                               col("Students.user.Location"),
                               col("Students.user.Name"),
                               col("Brand")
                               
                           )
    flatdata.show()
   flatdata.printSchema()
    
    val complexdata = flatdata.select(
                                struct(    
                                      col("Permanent address"),
                                      col("current Address")
                                  ).alias("Address"),
                                 col("Mobile"),
                                 col("Name"),
                                 col("status"),
                                 col("Pets"),
                                 col("Location"),
                                 col("Student Name"),
                                 col("Brand")
                                    
                                )
    complexdata.printSchema()
    complexdata.show()
    
    val finalComplexdata1 = complexdata.groupBy("Address","Mobile","Name","Student Name","status","Location","Brand")
    .agg(collect_list(col("Pets")).alias("Pets"))
    
    finalComplexdata1.printSchema()
    
      
    val innerarray = finalComplexdata1.groupBy("Address","Mobile","Student Name","status","Pets")
    .agg(collect_list(col("Brand")).alias("Brand"))
    .select("Brand")
       
        innerarray.show()
        innerarray.printSchema()
    
  val finalComplexdata2 = finalComplexdata1.groupBy("Address","Mobile","Student Name","status","Pets")
    .agg(collect_list(
            struct(
                struct(
                    
                    col("Location"),
                    array(col("Brand")).alias("Brand"),
                   
                    col("Name")
                ).alias("user")
           )
       ).alias("Students")
    )
    
    finalComplexdata2.show()
    finalComplexdata2.printSchema()


    
  }
  
  
}