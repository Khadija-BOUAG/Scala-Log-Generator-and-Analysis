import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.streaming
import org.apache.spark.sql.functions._
import org.apache.log4j._
import java.io.File

object Go {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]").appName("log")
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      .config("es.index.auto.create", "true") // this will ensure that index is also created on first POST
      .config("es.nodes.wan.only", "true") // needed to run against dockerized ES for local tests
      .config("es.net.http.auth.user", "elastic")
      .config("es.net.http.auth.pass", "changeme")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val schema = StructType(List(
      StructField("Protocol", StringType , nullable = false),
      StructField("Http", IntegerType , nullable = true),
      StructField("Url", StringType, nullable = true),
      StructField("Path", StringType, nullable = true),
      StructField("Ip", StringType, nullable = true)))

    val StreamDF = spark.readStream.option("delimiter", " ").schema(schema)
      .csv("/home/dba/IdeaProjects/SparkStreamingPro/data/")
    StreamDF.createOrReplaceTempView("SDF")
    val outDF = spark.sql("select Http, Url, Path, Ip from SDF")

    outDF.writeStream
      .outputMode("append")
      .format("es")
      .option("checkpointLocation", "/tmp/checkpointLocation")
      .start("logger").awaitTermination()




  }

}

