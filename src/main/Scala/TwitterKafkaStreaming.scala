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

class TwitterKafkaStreaming {

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
    client_hbc.connect()

    while (!client_hbc.isDone) {
      val tweets : String = queue.poll(15, TimeUnit.SECONDS)
      getProducerKafka(kafkaBootStrapServers, topic, tweets)     // intégration avec notre producer Kafka
      println( "message Twitter : " + tweets)
    }


  }





}
