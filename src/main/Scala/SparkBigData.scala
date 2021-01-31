import java.io.FileNotFoundException

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.streaming._
import org.apache.log4j._
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.UserDefinedFunction


object SparkBigData {

  // Developpement d'applications Big Data en Spark

  var ss : SparkSession = null
  var spConf : SparkConf = null

  private var trace_log : Logger = LogManager.getLogger("Logger_Console")

  val schema_order = StructType(Array(
    StructField("orderid",IntegerType, false),
    StructField("customerid",IntegerType, false),
    StructField("campaignid",IntegerType, true),
    StructField("orderdate",TimestampType, true),
    StructField("city",StringType, true),
    StructField("state",StringType, true),
    StructField("zipcode",StringType, true),
    StructField("paymenttype",StringType, true),
    StructField("totalprice",DoubleType, true),
    StructField("numorderlines",IntegerType, true),
    StructField("numunit",IntegerType, true)
  ))

  def main(args: Array[String]) : Unit = {

    val session_s = Session_Spark(true)

    session_s.udf.register("valid_phone",valid_phoneUDF)

    val df_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\2010-12-06.csv")

    val df_gp = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\csvs\\")

    val df_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\2010-12-06.csv", "F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\2011-12-08.csv")

   // df_gp2.show(7)
   // println("df_gp count : "+ df_gp.count() + "  df_group2 count : " + df_gp2.count())

    df_test.printSchema()

    val df_2 =  df_test.select(
      col("InvoiceNo").cast(StringType),
      col("_c0").alias("ID du client"),
      col("StockCode").cast(IntegerType).alias("code de la marchandise"),
      col("Invoice".concat("No")).alias("ID de la commande")
    )

    val df_3 = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
        .withColumn("StockCode", col("StockCode").cast(IntegerType))
        .withColumn("valeur_constante", lit(50))
        .withColumnRenamed("_c0", "ID_client")
        .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("ID_client")))
        .withColumn("total_amount", round(col("Quantity") * col("UnitPrice"), 2))
        .withColumn("Created_dt", current_timestamp())
        .withColumn("reduction_test", when(col("total_amount") > 15, lit(3)).otherwise(lit(0)))
        .withColumn("reduction", when(col("total_amount") < 15, lit(0))
                                         .otherwise(when(col("total_amount").between(15, 20), lit(3))
                                         .otherwise(when(col("total_amount") > 15, lit(4)))))
        .withColumn("net_income", col("total_amount") - col("reduction"))


    val df_notreduced = df_3.filter(col("reduction") === lit(0) && col("Country").isin("United Kingdom", "France", "USA"))

    //jointures de data frame
    val df_orders = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .load("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\orders.txt")

   val df_ordersGood =  df_orders.withColumnRenamed("numunits", "numunits_order")
      .withColumnRenamed("totalprice", "totalprice_order")

    val df_products = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\product.txt")

    val df_orderlines = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\orderline.txt")

    val df_joinOrders =  df_orderlines.join(df_ordersGood, df_ordersGood.col("orderid") === df_orderlines.col("orderid"), "inner")
        .join(df_products, df_products.col("productid") === df_orderlines.col("productid"), Inner.sql )

    df_joinOrders.printSchema()

    val df_fichier1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\2010-12-06.csv")

    val df_fichier2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\2011-01-20.csv")

    val df_fichier3 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\2011-12-08.csv")

    val df_unitedfiles = df_fichier1.union(df_fichier2.union(df_fichier3))
   // println( df_fichier3.count() + " " + df_unitedfiles.count())

    df_joinOrders.withColumn("total_amount", round(col("numunits") * col("totalprice"), 3))
      .groupBy("state", "city")
      .sum("total_amount").as("Commandes totales")

    //opérations de fenêtrage
    val wn_specs = org.apache.spark.sql.expressions.Window.partitionBy(col("state"))
    val df_windows = df_joinOrders.withColumn("ventes_dep", sum(round(col("numunits") * col("totalprice"), 3)).over(wn_specs))
      .select(
        col("orderlineid"),
        col("zipcode"),
        col("PRODUCTGROUPNAME"),
        col("state"),
        col("ventes_dep").alias("Ventes_par_département")
      )

    // manipulation des dates et du temps en Spark
    df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MMMM/yyyy hh:mm:ss"))
      .withColumn("date_lecture_complete", current_timestamp())
      .withColumn("periode_jour", window(col("orderdate"), "10 minutes"))
      .select(
        col("orderdate"),
        col("periode_jour"),
        col("periode_jour.start"),
        col("periode_jour.end")
      )

    df_unitedfiles.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
      .withColumn("InvoiceTimestamp", col("InvoiceTimestamp").cast(TimestampType))
      .withColumn("Invoice_add_2months", add_months(col("InvoiceDate"), 2))
      .withColumn("Invoice_add_date", date_add(col("InvoiceDate"), 30))
      .withColumn("Invoice_sub_date", date_sub(col("InvoiceDate"), 25))
      .withColumn("Invoice_date_diff", datediff(current_date(), col("InvoiceDate")))
      .withColumn("InvoiceDateQuarter", quarter(col("InvoiceDate")))
      .withColumn("InvoiceDate_id", unix_timestamp(col("InvoiceDate")))
      .withColumn("InvoiceDate_format", from_unixtime(unix_timestamp(col("InvoiceDate")), "dd-MM-yyyy"))

    df_products
      .withColumn("productGp", substring(col("PRODUCTGROUPNAME"),2, 2))
      .withColumn("productln", length(col("PRODUCTGROUPNAME")))
      .withColumn("concat_product", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
      .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))
      .where(regexp_extract(trim(col("PRODUCTID")),"[0-9]{5}",0) === trim(col("PRODUCTID")))
      .where(! col("PRODUCTID").rlike("[0-9]{5}"))

    import session_s.implicits._
    val phone_list : DataFrame = List("0709789485", "+3307897025", "8794O07834").toDF("phone_number")

    phone_list
      .withColumn("test_phone", valid_phoneUDF(col("phone_number")))
     // .show()

    phone_list.createOrReplaceTempView("phone_table")
    session_s.sql( "select valid_phone(phone_number) as valid_phone from phone_table")
        .show()


   /* df_windows
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\Ecriture")

    //exemple de propriétés d'un format
    df_2.write
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .orc("users_with_options.orc")
  */

    df_joinOrders.createOrReplaceTempView("orders")

    val df_sql : DataFrame = session_s.sql("""
      select state, city, sum(round(numunits * totalprice)) as commandes_totales from orders group by state, city
      """)

    val df_hive = session_s.table("orders")    // lire une table à partir du metastore Hive
    df_sql.write.mode(SaveMode.Overwrite).saveAsTable("report_orders")  // enregistrer et écrire un data frame dans le metastore Hive

  }

  def valid_phone(phone_to_test : String): Boolean = {

    var result : Boolean = false
    val motif_regexp = "^0[0-9]{9}".r

     if (motif_regexp.findAllIn(phone_to_test.trim) == phone_to_test.trim) {
       result = true
     } else {
       result = false
     }

    return result

  }

  val valid_phoneUDF : UserDefinedFunction = udf{(phone_to_test : String) => valid_phone(phone_to_test)}

  def spark_hdfs () : Unit = {

    val config_fs = Session_Spark(true).sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config_fs)

    val src_path = new Path("/user/datalake/marketing/")
    val dest_path = new Path("/user/datalake/indexes")
    val ren_src = new Path("/user/datalake/marketing/fichier_reporting.parquet")
    val dest_src = new Path("/user/datalake/massrketing/reporting.parquet")
    val local_path = new Path("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame\\Ecriture\\parts.csv")
    val path_local = new Path("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Data frame")

    // lecture des fichiers d'un dossier
    val files_list = fs.listStatus(src_path)
    files_list.foreach(f => println(f.getPath))

    val files_list1 = fs.listStatus(src_path).map(x => x.getPath)
    for( i <- 1 to files_list1.length) {
      println(files_list1(i))
    }

    // renommage des fichiers
   fs.rename(ren_src, dest_src)

    //supprimer des fichiers dans un dossier
    fs.delete(dest_src, true)

    //copie des fichiers
    fs.copyFromLocalFile(local_path, dest_path)
    fs.copyToLocalFile(dest_path, path_local)

  }

  def manip_rdd() : Unit = {

    val session_s = Session_Spark(true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")

    val rdd_test : RDD[String] = sc.parallelize(List("alain", "juvenal", "julien", "anna"))

    rdd_test.foreach{
      l =>  println(l)
    }
    println()

    val rdd2 : RDD[String] = sc.parallelize(Array("Lucie", "fabien", "jules"))
    rdd2.foreach{ l => println(l)}
    println()

    val rdd3 = sc.parallelize(Seq(("Julien", "Math", 15), ("Aline", "Math", 17), ("Juvénal", "Math", 19)))
    println("Premier élément de mon RDD 3")
    rdd3.take(1).foreach{ l => println(l)}

    if(rdd3.isEmpty()) {
      println("le RDD est vide")
    } else {
      rdd3.foreach{
        l => println(l)
      }
    }

    rdd3.saveAsTextFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\rdd.txt")

    rdd3.repartition(1).saveAsTextFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\rdd3.txt")

    rdd3.foreach{l => println(l)}
    rdd3.collect().foreach{l => println(l)}

    // création d'un RDD à partir d'une source de données
    val rdd4 = sc.textFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\textRDD.txt")
    println("lecture du contenu du RDD4")
    rdd4.foreach{l => println(l)}

    val rdd5 = sc.textFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\*")
    println("lecture du contenu du RDD5")
    rdd5.foreach{l => println(l)}

    // transformation des RDD
    val rdd_trans : RDD[String] = sc.parallelize(List("alain mange une banane", "la banane est un bon aliment pour la santé", "acheter une bonne banane"))
    rdd_trans.foreach(l => println( "ligne de mon RDD : " + l))

    val rdd_map = rdd_trans.map(x => x.split(" "))
    println("Nbr d'elements de mon RDD Map : " + rdd_map.count())

    val rdd6 = rdd_trans.map(w => (w, w.length, w.contains("banane"))).map(x => (x._1.toLowerCase, x._2, x._3))

    val rdd7 = rdd6.map(x => (x._1.toUpperCase(), x._3, x._2))

    val rdd8 = rdd6.map(x => (x._1.split(" "), 1) )
    rdd8.foreach(l => println(l._1(0), l._2))

    val rdd_fm = rdd_trans.flatMap(x => x.split(" ")).map(w => (w, 1))

    val rdd_compte = rdd5.flatMap(x => x.split(" ")).map(m => (m, 1)).reduceByKey((x, y) => x + y)
    rdd_compte.repartition(1).saveAsTextFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\ComptageRDD.txt")
    rdd_compte.foreach(l => println(l))

    val rdd_filtered = rdd_fm.filter(x => x._1.contains("banane"))

    val rdd_reduced = rdd_fm.reduceByKey((x, y) => x + y)
    rdd_reduced.foreach(l => println(l))

    rdd_fm.cache()
    rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_fm.unpersist()

    import session_s.implicits._
    val df : DataFrame = rdd_fm.toDF("texte", "valeur")
    df.show(50)

  }


  /**
   * fonction qui initialise et instancie une session spark
   * @param env : c'est une variable qui indique l'environnement sur lequel notre application est déployée.
   *            Si Env = True, alors l'appli est déployée en local, sinon, elle est déployée sur un cluster
   */
  def Session_Spark (env :Boolean = true) : SparkSession = {
    try {
     if (env == true) {
       System.setProperty("hadoop.home.dir", "C:/Hadoop/")
       ss = SparkSession.builder
           .master("local[*]")
           .config("spark.sql.crossJoin.enabled", "true")
       //    .enableHiveSupport()
           .getOrCreate()
     }else {
       ss  = SparkSession.builder
         .appName("Mon application Spark")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.crossJoin.enabled", "true")
         .enableHiveSupport()
         .getOrCreate()
     }
    } catch {
      case ex : FileNotFoundException => trace_log.error("Nous n'avons pas trouvé le winutils dans le chemin indiqué " + ex.printStackTrace())
      case ex : Exception  => trace_log.error("Erreur dans l'initialisation de la session Spark " + ex.printStackTrace())
    }

    return ss

  }


  /**
   * fonction qui initialise le contexte Spark Streaming
   * @param env : environnement sur lequel est déployé notre application. Si true, alors on est en localhost
   * @param duree_batch : c'est le SparkStreamingBatchDuration - où la durée du micro-batch
   * @return : la fonction renvoie en résultat une instance du contexte Streaming
   */

  def getSparkStreamingContext (env : Boolean = true, duree_batch : Int) : StreamingContext = {
    trace_log.info("initialisation du contexte Spark Streaming")
    if (env) {
      spConf = new SparkConf().setMaster("local[*]")
          .setAppName("Mon application streaming")
    } else {
      spConf = new SparkConf().setAppName("Mon application streaming")
    }
    trace_log.info(s"la durée du micro-bacth Spark est définie à : $duree_batch secondes")
    val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

    return ssc

  }


}
