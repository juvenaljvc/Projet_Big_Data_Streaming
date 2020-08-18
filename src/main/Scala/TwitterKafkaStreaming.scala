import java.time.Duration

import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.twitter.hbc.core.endpoint.{Endpoint, StatusesFilterEndpoint}
import java.util.Collections

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor

import scala.collection.JavaConverters._
import KafkaStreaming._
import org.apache.log4j.{LogManager, Logger}

class TwitterKafkaStreaming {

  private var trace_client_streaming : Logger = LogManager.getLogger("Log_Console")

  /**
   * Ce client est un client Hosebird. Il permet de collecter les tweets contenant une liste d'hashtag et de les publier en temps réel dans un ou plusieurs topics kafka
   * @param CONSUMER_KEY : la clé du consommateur pour l'authentification OAuth
   * @param CONSUMER_SECRET : le secret du consommateur pour l'authentification OAuth
   * @param ACCESS_TOKEN : le token d'accès pour l'authentification OAuth
   * @param TOKEN_SECRET : le token secret pour l'authentification OAuth
   * @param liste_hashtags : la liste des hastags de tweets dont on souhaite collecter
   * @param kafkaBootStrapServers : la liste d'adresses IP (et leur port) des agents du cluster Kafka
   * @param topic : le(s) topic(s) dans le(s)quel(s) stocker les tweets collectés
   */

  def ProducerTwitterKafkaHbc (CONSUMER_KEY : String, CONSUMER_SECRET : String, ACCESS_TOKEN : String, TOKEN_SECRET : String, liste_hashtags : String,
                               kafkaBootStrapServers : String, topic : String) : Unit = {

    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](10000)

    val auth : Authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET)

    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.trackTerms(Collections.singletonList(liste_hashtags))
    endp.trackTerms(List(liste_hashtags).asJava)   //Collections.singletonList

    val constructeur_hbc : ClientBuilder = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .endpoint(endp)
      .processor(new StringDelimitedProcessor(queue)
      )

    val client_hbc : Client = constructeur_hbc.build()

    try {
      client_hbc.connect()

      while (!client_hbc.isDone) {
        val tweets : String = queue.poll(15, TimeUnit.SECONDS)
        getProducerKafka(kafkaBootStrapServers, topic, tweets)     // intégration avec notre producer Kafka
        println( "message Twitter : " + tweets)
      }
    } catch {

      case ex : InterruptedException => trace_client_streaming.error("le client Twitter HBC a été interrompu à cause de cette erreur : " + ex.printStackTrace())

    } finally {

      client_hbc.stop()
      getProducerKafka(kafkaBootStrapServers, topic, "").close()

    }

  }





}
