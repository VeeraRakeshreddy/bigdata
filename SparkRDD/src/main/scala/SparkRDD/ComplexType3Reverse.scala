package SparkRDD


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object ComplexType3Reverse {

  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    
    val petsdata = spark.read.format("json").option("multiline","true").load("file:///C:/data/petsjson.json")
    
    petsdata.show()
    petsdata.printSchema()
    
    val flattcomplex = petsdata.withColumn("Pets",explode(col("Pets")) )
    .select(
        "Address.*", 
         "Mobile",
         "Name",
         "status" ,
         "Pets"
    )
    
    flattcomplex.show()
    flattcomplex.printSchema()
    
    val revstructcomplex = flattcomplex.select(
                struct(
                    col("Permanent address"),
                    col("current Address")
                    ).alias("Address"),
                col("Mobile"),
                col("Name"),
                col("status"),
                col("Pets")
                )
                
          revstructcomplex.show()
          revstructcomplex.printSchema()
    
    val revflattcomplex = revstructcomplex
            .groupBy("Address", "Mobile","Name","status")
                .agg(
                    collect_list(col("Pets")).alias("Pets")
                    )
            
               
                .select("Address","Mobile","Name","Pets","status")    
                    
           revflattcomplex.show()
           revflattcomplex.printSchema()
                
    

  }
}