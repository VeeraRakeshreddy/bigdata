package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._



/*import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType*/





object SparkRevision {
  
  //Reg:-4
  case class schema ( Country_Code:String,
                      Population: String,
                      GDP: String,
                      Capital : String,
                      Country : String
                      )
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    //Reg:-5
    val schema2 = StructType(Array(
        StructField("Country_Code",StringType),
        StructField("Population",StringType),
        StructField("GDP",StringType),
        StructField("Capital",StringType),
        StructField("Country",StringType)
        ) )
        
     //Reg:-6
        
        val unifiedorder = List("Country","Email","First_Name","Last_Name","Phone_Number","City","Age")
    
    // Rev:- 1
    val mylist = List(1,4,6,7)
    val Result = mylist.map(x=>x+2)
    Result.foreach(println)
    println
        
    //Rev:-2
    
    val scalalist = List("zeyobron","zeyo","analytics")
    val Result2 = scalalist.filter(x=>x.contains("zeyo"))
    Result2.foreach(println)
    println
        
    //Rev:-3
    
    val filerdd = sc.textFile("file:///C:/data/country_info.txt")
    val filterrdd = filerdd.filter(x=>x.contains("IND"))
    filterrdd.foreach(println)
    println
    
    //Rev:-4
    
    val mapsplit = filerdd.map(x=>x.split(","))
    val caseclassfilter = mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4)))
    val filtercaseclass = caseclassfilter.filter(x=>x.Capital=="New Delhi")
    filtercaseclass.foreach(println)
    println
    
    //Rev:-5
    
    val rowrdd = mapsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4)))
    
    rowrdd.foreach(println)
    
  
    
    //Rev:-6
    
    val dfschemardd = filterrdd.toDF()
    dfschemardd.show()
    
    val dfrowrdd = spark.createDataFrame(rowrdd,schema2)
    dfrowrdd.show()
    println
    
     //Rev:-7
    
    val csvdata = spark.read
    .format("csv")
    .option("header", "true")
    .load("file:///C:/data/contact_info.csv").select(unifiedorder.map(col):_*)
    csvdata.show()
    
    
    //Rev:-8
    
    val parquetdata = spark.read
    .load("file:///C:/data/parquetdata/parquetdata.parquet")
    .select(unifiedorder.map(col):_*)
    
    parquetdata.show()
    println
    
    val jsondata = spark.read
    .format("json")
    .load("file:///C:/data/jsondata/jdata.json")
    .select(unifiedorder.map(col):_*)
    
    
    jsondata.show()
    
    //Rev:-9
    
    val union = jsondata.union(csvdata).union(parquetdata)
    
    union.show(40)
    
    //Rev:-10
    
    val splitRenamefilter = union.withColumn("Phone_Number", expr("split(Phone_Number,'-')[0]"))
                                 .withColumnRenamed("Phone_Number", "Country_Code")
                                 .withColumn("Status",expr("case when Country = 'USA' then 'Good' else 'may be not' end "))
                                 .filter(col("Age")>40)
         
       splitRenamefilter.show()
       
     //Rev:-11
       
       val groupbycount = splitRenamefilter.groupBy("Age")
                                           .agg(count("*").alias("Total_Records"))
                                           .show()
    //Rev:- 12
                                           
    splitRenamefilter.write.format("com.databricks.spark.avro").mode("append").partitionBy("First_Name").save("file:///C:/data/revisionavrodata")
  }
  
  
}