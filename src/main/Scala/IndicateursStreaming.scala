import KafkaStreaming._
import SparkBigData._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object IndicateursStreaming {

  val  schema_indicateurs = StructType ( Array(
    StructField("event_date", DateType, true),
    StructField("id", StringType, false),
    StructField("text", StringType, true),
    StructField("lang", StringType, true),
    StructField("userid", StringType, false),
    StructField("name", StringType, false),
    StructField("screenName", StringType, true),
    StructField("location", StringType, true),
    StructField("followersCount", IntegerType, false),
    StructField("retweetCount", IntegerType, false),
    StructField("favoriteCount", IntegerType, false),
    StructField("Zipcode", StringType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true))
  )

  private val bootStrapServers : String = ""
  private val consumerGroupId : String = ""
  private val consumerReadOrder : String = ""
  private val zookeeper : String = ""
  private val kerberosName : String = ""
  private val batchDuration : Int = 600
  private val topics : Array[String] = Array("")
  private val path_fichiers_kpi : String = "/data lake/marketing_JVC/Projet Streaming/kpi"

  private var logger : Logger = LogManager.getLogger("Log_Console")

  def main(args: Array[String]): Unit = {

    val ssc = getSparkStreamingContext(true, batchDuration )

    val kk_consumer = getConsommateurKafka(bootStrapServers, consumerGroupId, consumerReadOrder, zookeeper, kerberosName, topics, ssc)

    kk_consumer.foreachRDD{

      rdd_kafka => {

        try {

          val event_kafka = rdd_kafka.map( e => e.value())
          val offsets_kafka = rdd_kafka.asInstanceOf[HasOffsetRanges].offsetRanges

          val ssession = SparkSession.builder.config(rdd_kafka.sparkContext.getConf).enableHiveSupport().getOrCreate()
          import ssession.implicits._

          val events_df = event_kafka.toDF("kafka_jsons")

          if( events_df.count() == 0) {

            Seq(
              ("Aucun événement n'a été receptionné dans le quart d'heure")
            ).toDF("libelé")
              .coalesce(1)
              .write
              .format("com.databricks.spark.csv")
              .mode(SaveMode.Overwrite)
              .save(path_fichiers_kpi + "/logs_streaming.csv")

          } else {

            val df_parsed = getParsedData(events_df, ssession)
            val df_kpi = getIndicateursComputed(df_parsed, ssession).cache()

            df_kpi.repartition(1)
              .write
              .format("com.databricks.spark.csv")
              .mode(SaveMode.Append)
              .save(path_fichiers_kpi + "/indicateurs_streaming.csv")

          }

          kk_consumer.asInstanceOf[CanCommitOffsets].commitAsync(offsets_kafka)

        } catch {

          case ex : Exception => logger.error(s"il y'a une ereur dans l'application ${ex.printStackTrace()}")

        }

      }

    }

    ssc.start()
    ssc.awaitTermination()

  }

  def getParsedData (kafkaEventsDf : DataFrame, ss : SparkSession) : DataFrame = {

    logger.info("parsing des json reçus de Kafka encours...")

    import ss.implicits._

    val df_events = kafkaEventsDf.withColumn("kafka_jsons", from_json(col("kafka_jsons"), schema_indicateurs))
      .filter(col("kafka_jsons.lang") === lit("en") || col("kafka_jsons.lang") === lit("fr"))
      .select(
        col("kafka_jsons.event_date"),
        col("kafka_jsons.id"),
        col("kafka_jsons.text"),
        col("kafka_jsons.lang"),
        col("kafka_jsons.userid"),
        col("kafka_jsons.name"),
        col("kafka_jsons.screenName"),
        col("kafka_jsons.location"),
        col("kafka_jsons.followersCount"),
        col("kafka_jsons.retweetCount"),
        col("kafka_jsons.favoriteCount"),
        col("kafka_jsons.Zipcode"),
        col("kafka_jsons.ZipCodeType"),
        col("kafka_jsons.City"),
        col("kafka_jsons.State")
      )

    return df_events

  }

  def getIndicateursComputed (eventsDf_parsed : DataFrame, ss : SparkSession) : DataFrame = {

    logger.info("calcul des indicateurs encours....")

    import ss.implicits._

    eventsDf_parsed.createOrReplaceTempView("events_tweets")

    val df_indicateurs = ss.sql("""
           SELECT t.date_event,
               t.quart_heure,
               count(t.id) OVER (PARTITION BY t.date_event, t.quart_heure ORDER BY t.date_event,t.quart_heure) as tweetCount,
               sum(t.bin_retweet) OVER (PARTITION BY t.date_event, t.quart_heure ORDER BY t.date_event, t.quart_heure) as retweetCount

           FROM  (
              SELECT  from_unixtime(unix_timestamp(event_date, 'yyyy-MM-dd'), 'yyyy-MM-dd') as date_event,
                  CASE
                    WHEN minute(event_date) < 15 THEN CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "00"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "15"))
                    WHEN minute(event_date) between 15 AND 29 THEN  CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "15"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "30"))
                    WHEN minute(event_date) between 30 AND 44 THEN  CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "30"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "45"))
                    WHEN minute(event_date) > 44 THEN  CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "45"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "60"))
                  END AS quart_heure,
                  CASE
                    WHEN retweetCount > 0 THEN 1
                    ELSE 0
                  END AS bin_retweet,
                   id
              FROM events_tweets
           ) t ORDER BY t.quart_heure
      """
    ).withColumn("Niveau_RT", round(lit(col("retweetCount") / col("tweetCount")) * lit(100), 2))
     .withColumn("date_event", when(col("date_event").isNull, current_timestamp()).otherwise(col("date_event")))
     .select(
        col("date_event").alias("Date de l'event"),
        col("quart_heure").alias("Quart d'heure de l'event"),
        col("tweetCount").alias("Nbre de Tweets par QH"),
        col("retweetCount").alias("Nbre de Retweets par QH"),
        col("Niveau_RT").alias("Niveau de ReTweet (en %)")
      )

    return df_indicateurs

  }

}
