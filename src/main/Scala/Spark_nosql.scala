import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkBigData._

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.execution.datasources._

object Spark_nosql {

  def catalogue_table =
    s"""{
      |"table":{"namespace":"default", "name":"table_jvc"},
      |"rowkey":"key",
      |"columns":{
      |"key":{"cf":"rowkey", "col":"key", "type":"string"},
      |"first_col":{"cf":"col_family", "col":"1", "type":"string"},
      |"second_col":{"cf":"col_family", "col":"2", "type":"string"},
      |"third_col":{"cf":"col_family", "col":"3", "type":"string"}
      |}
      |}""".stripMargin

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(true)

    val df_hbase = ss.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalogue_table))
      .format("org.apache.spark.sql.execution.datasources.hbase")     // "org.apache.hadoop.hbase.spark"
      .load()

    df_hbase.printSchema()
    df_hbase.show()


  }

}
