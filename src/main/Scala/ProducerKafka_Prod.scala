import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.protocol
import org.apache.kafka.common.serialization._
import org.apache.kafka.common._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import java.util.Properties
import java.util.Collections

import KafkaStreaming.{getJSON, getKafkaProducerParams_exactly_once, trace_kafka}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.{ProducerRecord, _}
import org.apache.kafka.clients.producer.ProducerConfig._
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode

object ProducerKafka_Prod {

  private var trace_kafka : Logger = LogManager.getLogger("Log_Console")


  def main(args: Array[String]): Unit = {

    for( i <- 1 to 10 ) {
      ProducerKafka_prod("localhost:9092", "orderline")
    }
  }

  def ProducerKafka_prod(KafkaBootStrapServers : String, topic_name : String) : Unit = {

    val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams_prod(KafkaBootStrapServers))

    val record_publish =  getJSON(KafkaBootStrapServers : String, topic_name : String)

    try {
      trace_kafka.info("publication du message encours...")

      producer_Kafka.send(record_publish, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            //le message a été enregistré dans Kafka sans problème.
            println("offset du message : " + metadata.offset().toString)
            println("topic du message : " + metadata.topic().toString())
            println("partition du message : " + metadata.partition().toString())
            println("heure d'enregistrement du message : " + metadata.timestamp())
          }
        }
      } )
      println("message publié avec succès ! :)")
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
        trace_kafka.info("La liste des paramètres pour la connexion du Producer Kafka sont :" + getKafkaProducerParams_prod(KafkaBootStrapServers))
    }

  }


  def getKafkaProducerParams_prod (KafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBootStrapServers)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")

    return props

  }

  def getJSON(KafkaBootStrapServers : String, topic_name : String) : ProducerRecord[String, String] = {

    val objet_json = JsonNodeFactory.instance.objectNode()

    val price : Int = 45

    objet_json.put("orderid", "10")
    objet_json.put("customerid", "150")
    objet_json.put("campaignid", "30")
    objet_json.put("orderdate", "10/04/2020")
    objet_json.put("city", "Paris")
    objet_json.put("state", "RAS")
    objet_json.put("zipcode", "75000")
    objet_json.put("paymenttype", "CB")
    objet_json.put("totalprice", price)
    objet_json.put("numorderlines", 200)
    objet_json.put("numunit",10)

    return  new ProducerRecord[String, String](topic_name,objet_json.toString)

  }






}
