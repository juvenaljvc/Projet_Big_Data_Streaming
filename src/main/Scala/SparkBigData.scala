import java.util.logging.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming._


object SparkBigData {

   var ss : SparkSession = null
   var spConf : SparkConf = null
  /**
   * fonction qui initialise et instancie une session spark
   * @param Env : c'est une variable qui indique l'environnement sur lequel notre application est déployée.
   *            Si Env = True, alors l'appli est déployée en local, sinon, elle est déployée sur un cluster
   */
  def Session_Spark (Env : Boolean = true) : SparkSession = {
     if (Env == true) {
       System.setProperty("hadoop.home.dir", "C:/Hadoop")
       ss = SparkSession.builder
           .master("local[*]")
           .config("spark.sql.crossJoin.enabled", "true")
           .enableHiveSupport()
           .getOrCreate()
     }else {
       ss  = SparkSession.builder
         .appName("Mon application Spark")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.sql.crossJoin.enabled", "true")
         .enableHiveSupport()
         .getOrCreate()
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

    if (env) {
      spConf = new SparkConf().setMaster("LocalHost[*]")
          .setAppName("Mon application streaming")
    } else {
      spConf = new SparkConf().setAppName("Mon application streaming")
    }

    val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

    return ssc

  }


}
