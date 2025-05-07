package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.text.Format


object XMl {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
   /* val xmldata = spark.read
    .format("com.databricks.spark.xml")
    //.option("multiline", "true")
    .option("rowTag", "Contact")
    .load("file:///C:/data/contacts.xml")
    xmldata.show()
    
    xmldata.write.format("csv").save("file:///C:/data/XML_csvdata")*/
     println("===============Task-1 (Read data from XML file formate==============")
    
    val myXML = spark.read.format("com.databricks.spark.xml").option("rowTag", "Contact").load("file:///C:/data/contacts.xml")
    
    myXML.show()
    
    println("===============Task-2 ( Create a partition on CSV file data)==============")
    
    val pardata = spark.read.format("csv").option("header", "true").load("file:///C:/data/country_info.txt")
        
     val pdata = pardata.write.format("csv").partitionBy("Country","Capital").mode("overwrite").save("file:///C:/data/Part_Country")
     
     println("===============Task-3 (intro of using DSL for selecting 2 columns from the dataframe)=============")
     
     val dsldata = pardata.select("Country","GDP")
     dsldata.show()
     
  }
  
}