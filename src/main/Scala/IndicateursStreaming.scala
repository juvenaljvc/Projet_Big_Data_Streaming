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

  private var logger : Logger = LogManager.getLogger("Log_Console")

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

  def getIndicateursComputed (EventsDf : DataFrame, ss : SparkSession) : DataFrame = {

  }


}
