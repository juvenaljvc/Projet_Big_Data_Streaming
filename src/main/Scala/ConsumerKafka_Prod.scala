import java.time.Duration

import org.apache.kafka.clients.consumer.ConsumerConfig._
import scala.collection.JavaConverters._
import java.util.Properties
import java.util.Collections

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common._

object ConsumerKafka_Prod {

  def main(args: Array[String]): Unit = {

    getClientConsumerKafka("localhost:9092", "orderline")

  }


  def getKafkaConsumerParams(kafkaBootStrapServers: String): Properties = {
    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "jvc")

    return props

  }

  def getClientConsumerKafka(kafkaBootStrapServers: String, topic_list: String): Unit = {

    val consumer = new KafkaConsumer[String, String](getKafkaConsumerParams(kafkaBootStrapServers))

    try {

      consumer.subscribe(Collections.singletonList(topic_list)) //on pouvait aussi faire ceci : List(topic_list).asJava

      while (true) {
        val messages: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(3))
        if (!messages.isEmpty) {
          println("Nombre de messages collectés dans la fenêtre :" + messages.count())
          for (message <- messages.asScala) {
            println("Topic: " + message.topic() +
              ",Key: " + message.key() +
              ",Value: " + message.value() +
              ", Offset: " + message.offset() +
              ", Partition: " + message.partition())
          }

          try {
            consumer.commitAsync() // ou bien consumer.commitSync()
          } catch {
            case ex: CommitFailedException =>
              println("erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons bien reçu les données")
          }

        }
      }
    }
  }

}
