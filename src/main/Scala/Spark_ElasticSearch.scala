import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkBigData._
import java.util._
import org.elasticsearch.spark.sql._
import org.apache.commons.httpclient.HttpConnectionManager
import org.apache.commons.httpclient._

object Spark_ElasticSearch {

  def main(args: Array[String]): Unit = {

    val ss = SparkBigData.Session_Spark(true)

    val df_orders = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\orders_csv.csv")

    df_orders.write         // écriture des données du data frame dans un index d'ElasticSearch
      .format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .option("es.port", "9200")
      .option("es.nodes", "localhost")
      .option("es.net.http.auth.user", "elastic")
      .option("es.net.http.auth.pass", "xUbU8Hwj6SwCtgvt019P")
      .save("index_jvc/doc")

    // autre façon de faire
    val session_s = SparkSession.builder
      .appName("Mon application Spark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("es.port", "9200")
      .config("es.nodes", "localhost")
      .config("es.net.http.auth.user", "elastic")
      .config("es.net.http.auth.pass", "xUbU8Hwj6SwCtgvt019P")
      .config("es.nodes.wan.only", "true")     // option à rajouter si les données viennent d'un service Cloud comme Amazon S3
      .enableHiveSupport()

    df_orders.saveToEs("index_jvc/doc")




  }



}
