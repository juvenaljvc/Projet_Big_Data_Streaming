import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization._
import org.apache.kafka.common._
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import SparkBigData._
import org.apache.spark.streaming.dstream.InputDStream


object KafkaStreaming {

  var KafkaParam : Map[String, Object] = Map(null, null)


  def getKafkaParams ( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
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

  def getConsommateurKafka( kafkaBootStrapServers : String, KafkaConsumerGroupId : String, KafkaConsumerReadOrder : String,
                            KafkaZookeeper : String, KerberosName : String, batchDuration : Int,
                            KafkaTopics : Array[String]) : InputDStream[ConsumerRecord[String, String]] = {

    val ssc = getSparkStreamingContext(true, batchDuration)
    KafkaParam = getKafkaParams(kafkaBootStrapServers, KafkaConsumerGroupId , KafkaConsumerReadOrder ,KafkaZookeeper, KerberosName )

    val ConsommateurKafka : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](KafkaTopics, KafkaParam )
    )

    return ConsommateurKafka

  }





}
