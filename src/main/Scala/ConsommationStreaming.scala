import KafkaStreaming._
import SparkBigData._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object ConsommationStreaming {

  val bootStrapServers : String = ""
  val consumerGroupId : String = ""
  val consumerReadOrder : String = ""
  val zookeeper : String = ""
  val kerberosName : String = ""
  val batchDuration : Int = 15
  val topics : Array[String] = Array("")

  val schema_Kafka = StructType(Array(
    StructField("Zipcode", IntegerType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true)
  ))


  def main(args: Array[String]): Unit = {

    val ssc = getSparkStreamingContext(true, batchDuration)

    val kafkaStreams = getConsommateurKafka(bootStrapServers, consumerGroupId, consumerReadOrder, zookeeper, kerberosName, topics, ssc)

    //première méthode :  val dataStreams = kafkaStreams.map(record => record.value())

    // deuxième méthode (recommandée)

    kafkaStreams.foreachRDD{
      rddKafka => {
        if(!rddKafka.isEmpty()) {

          val dataStreams = rddKafka.map(record => record.value())

          val ss = SparkSession.builder.config(rddKafka.sparkContext.getConf).enableHiveSupport.getOrCreate()
          import ss.implicits._

          val df_kafka = dataStreams.toDF("tweet_message")

          df_kafka.createOrReplaceGlobalTempView("kafka_events")

          // 1 ère méthode d'exploitation du Data Frame et SQL avec Kafka
          val df_eventsKafka = ss.sql("select * from kafka_events")

          df_eventsKafka.show()

          // 2 ème méthode d'exploitation du Data Frame et SQL avec Kafka
          val df_eventsKafka_2 = df_kafka.withColumn("tweet_message", from_json(col("tweet_message"), schema_Kafka))
            .select(col("tweet_message.*"))
        }

      }


    }





    ssc.start()
    ssc.awaitTermination()





  }



}
