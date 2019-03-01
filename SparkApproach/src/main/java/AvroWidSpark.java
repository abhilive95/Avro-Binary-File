import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;


public class AvroWidSpark {
	public static void main(String[] args) throws IOException {

        SparkConf sparkConf = new SparkConf();          
        sparkConf.setAppName("AvroWidSpark");
        sparkConf.setMaster("local");
        SparkSession scc= SparkSession.builder().appName("AvroWidSpark").master("local[*]").config("spark.driver.cores", 1).getOrCreate();		        
        SQLContext sql = new SQLContext(scc);    	
        
        //Encoder<MetaData> UserEncoder = Encoders.bean(MetaData.class); 
        //Dataset<MetaData> df = scc.read().format("com.databricks.spark.avro") .load("C:/AVRO_WORK/SparkApproach/src/main/avro/livePerson.avro").as(UserEncoder);
        
        
    	//Dataset<Row> df = sql.read().format("com.databricks.spark.avro").load("C:/AVRO_WORK/SparkApproach/src/main/avro/users.avro");
    	Dataset<Row> df = sql.read().format("com.databricks.spark.avro").load("C:/AVRO_WORK/SparkApproach/src/main/avro/livePerson.avro");
        //df.schema();
        df.show();
        df.select("recordCollection").show();
        df.select("recordCollection").printSchema();
        
        
        df.createOrReplaceTempView("sample");
        Dataset<Row> wcf = scc.sql("select count(*) as total from sample");
        wcf.show();


        //df.write().mode(SaveMode.Append).format("com.databricks.spark.csv").partitionBy("|", "||").save("C:/AVRO_WORK/SparkApproach/src/main/resources/output.avro");
        
        //df.write().repartition(1).format("com.databricks.spark.csv").option("header", "true").save("C:/AVRO_WORK/SparkApproach/src/main/resources/output.csv");
        //dataFrame.write.avro("/tmp/output")
        
        //df.write.format("com.databricks.spark.avro").save(outputPath);
        //df.write().avro("C:/AVRO_WORK/SparkApproach/src/main/resources/output.text");
        //df.select("metaData.*").take(5);
        //df.write().format("com.databricks.spark.avro").save("C:/AVRO_WORK/SparkApproach/src/main/resources/output.avro");      
    }
}


/*import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

object AvroWithSpark {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("AvroWithSpark")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
	Dataset<Row> df = sqlContext.read().format("com.databricks.spark.avro")
    		    .load("C:/AVRO_WORK/SparkApproach/src/main/avro/livePerson.avro");
    
  }
}*/