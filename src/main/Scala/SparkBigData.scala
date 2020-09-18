import java.io.FileNotFoundException

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.log4j._
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object SparkBigData {

  // Developpement d'applications Big Data en Spark

   var ss : SparkSession = null
   var spConf : SparkConf = null

   private var trace_log : Logger = LogManager.getLogger("Logger_Console")

  def main(args: Array[String]): Unit = {

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

    // rdd3.saveAsTextFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\rdd.txt")

    // rdd3.repartition(1).saveAsTextFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\rdd3.txt")

    rdd3.foreach{l => println(l)}
   // rdd3.collect().foreach{l => println(l)}

    // création d'un RDD à partir d'une source de données
    val rdd4 = sc.textFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\textRDD.txt")
    println("lecture du contenu du RDD4")
   // rdd4.foreach{l => println(l)}

    val rdd5 = sc.textFile("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\*")
    println("lecture du contenu du RDD5")
   // rdd5.foreach{l => println(l)}

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
   // rdd_reduced.foreach(l => println(l))

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
        //   .enableHiveSupport()
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
      spConf = new SparkConf().setMaster("LocalHost[*]")
          .setAppName("Mon application streaming")
    } else {
      spConf = new SparkConf().setAppName("Mon application streaming")
    }
    trace_log.info(s"la durée du micro-bacth Spark est définie à : $duree_batch secondes")
    val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

    return ssc

  }


}
