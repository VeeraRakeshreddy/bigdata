package SparkRDD

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SRSW {
  
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Rakesh").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ss = SparkSession.builder().getOrCreate()
     import ss.implicits._
     
     val mydata = ss.read.format("csv").option("header","true").load("file:///C:/data/contact_info.csv")
     mydata.show()
     mydata.persist()
     mydata.createOrReplaceTempView("sdata")
     val fdata = ss.sql("select * from sdata where Age>=30")
     fdata.show()
     
     
     //data frame to Avro file format
     
     val writeavro = fdata.write.format("com.databricks.spark.avro").mode("ignore").save("file:///C:/data/avrodata")
     println("Data Saved !!!!")
      //data frame to Avro file read format
     /*val readavro = ss.read.format("com.databricks.spark.avro").load("file:///C:/data/avrodata")
     readavro.show()*/
     
  }
  
  
}