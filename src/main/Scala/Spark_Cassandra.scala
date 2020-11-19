import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkBigData._

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import org.w3c.dom.Document


object Spark_Cassandra {

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(true)

    ss.conf.set(s"ss.sql.catalog.jvc", "com.datastax.spark.connector.datasource.CassandraCatalog")
    ss.conf.set(s"ss.sql.catalog.jvc.spark.cassandra.connection.host", "127.0.0.1")

    ss.sparkContext.cassandraTable("demo", "spacecraft_journey_catalog")

    val df_cassandra = ss.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "demo", "table" -> "spacecraft_journey_catalog", "cluster" -> "journey_id"))
      .load()

    val df_cassandra2 = ss.read
      .cassandraFormat("spacecraft_journey_catalog", "demo","journey_id")
      .load()

    df_cassandra.printSchema()
    df_cassandra.explain()
    df_cassandra.show()

    df_cassandra.write
      .mode("append")
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "spacecraft_journey_catalog", "keyspace" -> "demo", "output.consistency.level" -> "ALL", "ttl" -> "10000000"))
      .save()



  }

}
