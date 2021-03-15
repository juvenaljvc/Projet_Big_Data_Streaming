import java.time.Duration

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
import java.util.Collections

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.{ProducerRecord, _}
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.clients.producer.ProducerConfig._

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode

/*
cet objet regroupe l'ensemble des méthodes et fonctions nécessaires :
1 - pour établir une connexion avec un cluster Kafka (localhost ou cluster)
2 - pour consommer des données provenant d'un ou plusieurs topic Kafka
3 - pour stocker les données dans Kafka
Vous apprenez ici à développer des applications  Kafka robustes avec Spark et Scala
 */

object KafkaStreaming {

  var KafkaParam : Map[String, Object] = Map(null, null)                         // mauvaise approche, car variable immutable
  var consommateurKafka : InputDStream[ConsumerRecord[String, String]] = null   //mauvaise approche, car variable immutable
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

  def getKafkaSparkConsumerParams ( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                       KafkaZookeeper : String, KerberosName : String) : Map[String, Object] = {
    KafkaParam = Map(
      "bootstrap.servers" -> kafkaBootStrapServers,
      "group.id"  -> KafkaConsumerGroupId,
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
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("bootstrap.servers ", KafkaBootStrapServers)
    props.put("security.protocol",  "SASL_PLAINTEXT")

   return props

  }

  def getKafkaProducerParams_exactly_once (KafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBootStrapServers)
    props.put("security.protocol",  "SASL_PLAINTEXT")
    //propriétés pour rendre le producer Exactly-Once
    props.put(ProducerConfig.ACKS_CONFIG,  "all")
    // pour la cohérence éventuelle. Doit être inférieur ou égal au facteur de réplication du topic dans lequel vous allez publier
    props.put("min.insync.replicas", "2")
    //rendre le producer idempotent
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put("max.in.flight.requests.per.connection", "3")

    return props

  }

  def getKafkaConsumerParams (kafkaBootStrapServers : String, KafkaConsumerGroupId : String) : Properties = {
    val props : Properties = new Properties()
    props.put("bootstrap.servers", kafkaBootStrapServers)
    props.put("auto.offset.reset", "latest")
    props.put("group.id",KafkaConsumerGroupId )
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    return props

  }

  def getClientConsumerKafka (kafkaBootStrapServers : String, KafkaConsumerGroupId : String, topic_list : String) : KafkaConsumer[String, String] = {

    trace_kafka.info("instanciation d'un consommateur Kafka...")
    val consumer = new KafkaConsumer[String, String](getKafkaConsumerParams(kafkaBootStrapServers , KafkaConsumerGroupId))

    try {

      consumer.subscribe(Collections.singletonList(topic_list))  //on pouvait aussi faire ceci : List(topic_list).asJava

      while(true) {
        val messages : ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(30))
        if(!messages.isEmpty) {
          trace_kafka.info("Nombre de messages collectés dans la fenêtre :" + messages.count())
          for(message <- messages.asScala) {
            println("Topic: " + message.topic() +
              ",Key: " + message.key() +
              ",Value: " + message.value() +
              ", Offset: " + message.offset() +
              ", Partition: " + message.partition())
          }

          try {
            consumer.commitAsync()  // ou bien consumer.commitSync()
          }  catch {
            case ex : CommitFailedException =>
              trace_kafka.error("erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons bien reçu les données")
          }


          // méthode de lecture 2
          /*   val messageIterateur = messages.iterator()
             while (messageIterateur.hasNext == true) {
               val msg = messageIterateur.next()
               println(msg.key() + msg.value() + msg.partition())
             } */

        }

      }
    } catch {
      case excpt : Exception =>
        trace_kafka.error("erreur dans le consumer" + excpt.printStackTrace())
    } finally {
      consumer.close()
    }

    return consumer

  }


  /**
   * création d'un Kafka Producer qui va être déployé en production
   * @param KafkaBootStrapServers : agents kafka sur lesquels publier le message
   * @param topic_name : topic dans lequel publier le message
   * @param message : message à publier dans le topic @topic_name
   * @return : renvoie un Producer Kafka
   */
  def getProducerKafka (KafkaBootStrapServers : String, topic_name : String, message : String) : KafkaProducer[String, String] = {

    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs :  ${KafkaBootStrapServers}")
    lazy val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams(KafkaBootStrapServers))

    trace_kafka.info(s"message à publier dans le topic ${topic_name},  ${message}")
    val cle : String = "1"
    val record_publish = new ProducerRecord[String, String](topic_name, cle, message)

    try {
      trace_kafka.info("publication du message encours...")
      producer_Kafka.send(record_publish)

      trace_kafka.info("message publié avec succès ! :)")
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
        trace_kafka.info("La liste des paramètres pour la connexion du Producer Kafka sont :" + getKafkaProducerParams(KafkaBootStrapServers))
    } finally {
      println("n'oubliez pas de clôturer le Producer à la fin de son utilisation")
    }

    return producer_Kafka

  }


  def ProducerKafka_exactly_once (KafkaBootStrapServers : String, topic_name : String) : KafkaProducer[String, String] = {

    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs :  ${KafkaBootStrapServers}")
    val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams_exactly_once(KafkaBootStrapServers))

    val record_publish =  getJSON(KafkaBootStrapServers : String, topic_name : String)

    try {
      trace_kafka.info("publication du message encours...")

      producer_Kafka.send(record_publish, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            //le message a été enregistré dans Kafka sans problème.
            trace_kafka.info("offset du message : " + metadata.offset().toString)
            trace_kafka.info("topic du message : " + metadata.topic().toString())
            trace_kafka.info("partition du message : " + metadata.partition().toString())
            trace_kafka.info("heure d'enregistrement du message : " + metadata.timestamp())
          }
        }
      } )
      trace_kafka.info("message publié avec succès ! :)")
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
        trace_kafka.info("La liste des paramètres pour la connexion du Producer Kafka sont :" + getKafkaProducerParams_exactly_once(KafkaBootStrapServers))
    } finally {
      println("n'oubliez pas de clôturer le Producer à la fin de son utilisation")
    }

    return producer_Kafka

  }

  def getJSON(KafkaBootStrapServers : String, topic_name : String) : ProducerRecord[String, String] = {

    val objet_json = JsonNodeFactory.instance.objectNode()

    val price : Int = 45

    objet_json.put("orderid", "")
    objet_json.put("customerid", "")
    objet_json.put("campaignid", "")
    objet_json.put("orderdate", "")
    objet_json.put("city", "")
    objet_json.put("state", "")
    objet_json.put("zipcode", "")
    objet_json.put("paymenttype", "CB")
    objet_json.put("totalprice", price)
    objet_json.put("numorderlines", 200)
    objet_json.put("numunit",10)

    return  new ProducerRecord[String, String](topic_name,objet_json.toString)

  }

  /**
   *
   * @param kafkaBootStrapServers : adresse IP des agents Kafka
   * @param KafkaConsumerGroupId : ID du consummer Group
   * @param KafkaConsumerReadOrder :  ordre de lecture des données du Log
   * @param KafkaZookeeper : ensemble Zookeeper
   * @param KerberosName : service kerberos
   * @param KafkaTopics : le nom des topics
   * @return
   */

  def getConsommateurKafka( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                            KafkaZookeeper : String, KerberosName : String,
                            KafkaTopics : Array[String], StreamContext : StreamingContext) : InputDStream[ConsumerRecord[String, String]] = {
    try {

      KafkaParam = getKafkaSparkConsumerParams(kafkaBootStrapServers, KafkaConsumerGroupId , KafkaConsumerReadOrder ,KafkaZookeeper, KerberosName )

       consommateurKafka = KafkaUtils.createDirectStream[String, String](
        StreamContext,
        PreferConsistent,
        Subscribe[String, String](KafkaTopics, KafkaParam )
      )

    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans l'initialisation du consumer Kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramètres pour la connexion du consommateur Kafka sont : ${KafkaParam}")
    }

    return consommateurKafka

  }

}
