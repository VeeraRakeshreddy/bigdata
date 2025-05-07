package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Task1 {
  
  case class mycolumn(
    Country_Code:String,
    Population:String,
    GDP:String,
    Capital:String,
    Code:String
  )
  
  def main(args:Array[String]):Unit={
    
  val con = new SparkConf().setAppName("Rakesh").setMaster("local[*]")
  val sc =new SparkContext(con)
  sc.setLogLevel("ERROR")
  
  val data = sc.textFile("file:///C:/data/txns.txt")
  
  
  val mapsplit = data.map(z=>z.split("\\|"))
  
  val fildata = mapsplit.filter(z=>z(1).contains("70"))  

  
  fildata.foreach(println)

  val strdata = fildata.map(a=>a.mkString("~"))
  strdata.foreach(println)
   val RDDschema = mapsplit.map(x=>mycolumn(x(0),x(1),x(2),x(3),x(4)))
   
  /* val fildata = RDDschema.filter(x=>x.Code.contains("I"))
   
   fildata.foreach(println)*/
   
   //Data frame 
   
   ///Rakesh 
  
   
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = RDDschema.toDF()
    df.show()
    df.createOrReplaceTempView("Myview")
    val filterdeta = spark.sql("select * from Myview where Country_Code ='I'")
  filterdeta.show()
}
  
}