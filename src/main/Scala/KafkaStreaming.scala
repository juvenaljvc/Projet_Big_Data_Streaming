import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.protocol
import org.apache.kafka.common.serialization._
import org.apache.kafka.common._
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import SparkBigData._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import java.util.Properties
import org.apache.kafka.clients.producer._

/*
cet objet regroupe l'ensemble des méthodes et fonctions nécessaires :
1 - pour établir une connexion avec un cluster Kafka (localhost ou cluster)
2 - pour consommer des données provenant d'un ou plusieurs topic Kafka
3 - pour stocker les données dans Kafka
 */

object KafkaStreaming {

  var KafkaParam : Map[String, Object] = Map(null, null)
  var ConsommateurKafka : InputDStream[ConsumerRecord[String, String]] = null
  private var trace_kafka : Logger = LogManager.getLogger("Log_Console")


  /**
   * cette fonction récupère les paramètres de connexion à un cluster Kafka
   * @param kafkaBootStrapServers : adresses IP (avec port) des agents du cluster Kafka
   * @param KafkaConsumerGroupId : c'est l'ID du consumer group
   * @param KafkaConsumerReadOrder : l'ordre de lecture du Log
   * @param KafkaZookeeper : l'adresse IP (avec port) de l'ensemble ZooKeeper
   * @param KerberosName : le nom du service Kerberos
   * @return : la fonction renvoie une table clé-valeur des paramètres de connexion à un cluster Kafka spécifique
   */

  def getKafkaConsumerParams ( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                       KafkaZookeeper : String, KerberosName : String) : Map[String, Object] = {
    KafkaParam = Map(
      "bootstrap.servers" -> kafkaBootStrapServers,
      "groupe.id"  -> KafkaConsumerGroupId,
      "zookeeper.hosts" -> KafkaZookeeper,
      "auto.offset.reset" -> KafkaConsumerReadOrder,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" -> KerberosName,
      "security.protocol" -> SecurityProtocol.PLAINTEXT
      )

      return KafkaParam

  }

  def getKafkaProducerParams (KafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("acks", "all")
    props.put("bootstrap.servers ", KafkaBootStrapServers)
    props.put("security.protocol",  "SASL_PLAINTEXT")

   return props

  }

  /**
   * création d'un Kafka Producer qui va être déployé en production
   * @param KafkaBootStrapServers : agents kafka sur lesquels publier le message
   * @param topic_name : topic dans lequel publier le message
   * @param message : message à publier dans le topic @topic_name
   * @return : renvoie un Producer Kafka
   */
  def getProducerKafka (KafkaBootStrapServers : String, topic_name : String, message : String) : KafkaProducer[String, String] = {

    trace_kafka.info("instanciation d'une instance du producer Kafka aux serveurs" + ${KafkaBootStrapServers})
    lazy val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams(KafkaBootStrapServers))

    trace_kafka.info(s"message à publier dans le topic ${topic_name},  ${message}")
    val record_publish = new ProducerRecord[String, String](topic_name, message)

    try {
      trace_kafka.info("publication du message")
      producer_Kafka.send(record_publish)
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramètres pour la connexion du Producer Kafka sont : ${getKafkaProducerParams(KafkaBootStrapServers)}")
    } finally {
      producer_Kafka.close()
    }

    return producer_Kafka

  }

  /**
   *
   * @param kafkaBootStrapServers : adresse IP des agents Kafka
   * @param KafkaConsumerGroupId : ID du consummer Group
   * @param KafkaConsumerReadOrder :  ordre de lecture des données du Log
   * @param KafkaZookeeper : ensemble Zookeeper
   * @param KerberosName : service kerberos
   * @param batchDuration : la durée du streaming
   * @param KafkaTopics : le nom des topics
   * @return
   */

  def getConsommateurKafka( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                            KafkaZookeeper : String, KerberosName : String, batchDuration : Int,
                            KafkaTopics : Array[String]) : InputDStream[ConsumerRecord[String, String]] = {
    try {

      val ssc = getSparkStreamingContext(true, batchDuration)
      KafkaParam = getKafkaConsumerParams(kafkaBootStrapServers, KafkaConsumerGroupId , KafkaConsumerReadOrder ,KafkaZookeeper, KerberosName )

       ConsommateurKafka = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](KafkaTopics, KafkaParam )
      )

    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans l'initialisation du consumer Kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramètres pour la connexion du consommateur Kafka sont : ${KafkaParam}")
    }

     return ConsommateurKafka

  }





}
