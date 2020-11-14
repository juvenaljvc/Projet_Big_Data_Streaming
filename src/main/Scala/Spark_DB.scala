import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import SparkBigData._
import java.util._

object Spark_DB {

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(true)
    val props_mysql = new Properties()
    props_mysql.put("user", "consultant")
    props_mysql.put("password", "pwd#86")

    val props_postgreSQL = new Properties()
    props_postgreSQL.put("user", "postgres")
    props_postgreSQL.put("password", "pwd#86")

    val props_SQLServer = new Properties()
    props_SQLServer.put("user", "consultant")
    props_SQLServer.put("password", "pwd#86")

    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jea_db?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC","jea_db.orders", props_mysql)

    val df_mysql2 = ss.read
     .format("jdbc")
     .option("url", "jdbc:mysql://127.0.0.1:3306?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC")
     .option("user", "consultant")
     .option("password", "pwd#86")  //
     .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) table_summary")
     .load()

     df_mysql2.show()

     val df_postgre = ss.read.jdbc("jdbc:postgresql://127.0.0.1:5432/jea_db","orders", props_postgreSQL)

    val df_postgre2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/jea_db")
      .option("user", "postgres")
      .option("password", "pwd#86")  //
      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from orders group by state, city) table_postgresql")
      .load()

    df_postgre2.show()

   val df_sqlserver = ss.read.jdbc("jdbc:sqlserver://JUVENAL\\SPARK_SERVER:1433;databaseName=jea_db;","orders",props_SQLServer)

    val df_sqlserver1  = ss.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://JUVENAL\\SPARK_SERVER:1433;databaseName=jea_db;integratedSecurity=true")
      .option("dbtable", "(select state, city, sum(numunits * totalprice) as commandes_totales from orders group by state, city) table_sqlserver")
      .load()

    df_sqlserver.show(10)


  }

}
