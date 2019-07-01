package utad.flink.examples

import java.io.FileInputStream
import java.util.{Properties, StringTokenizer}

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

object Twitter extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val twitterCredentials = new Properties()
  twitterCredentials.load(new FileInputStream("/Users/miguelangelfernandezdiaz/workspace/twitter.properties"))

  env.setParallelism(1)

  val streamSource: DataStream[String] = env.addSource(new TwitterSource(twitterCredentials))

  val tweets: DataStream[(String, Int)] = streamSource.flatMap(new SelectEnglishAndTokenizeFlatMap).keyBy(0).sum(1)

  //tweets.writeAsText(s"/tmp/flink/twitter/${System.currentTimeMillis}")
  tweets.print()

  env.execute("Twitter Streaming Example")
}

/**
  * Deserialize JSON from twitter source
  *
  * Implements a string tokenizer that splits sentences into words as a
  * user-defined FlatMapFunction. The function takes a line (String) and
  * splits it into multiple pairs in the form of "(word,1)" ({{{ Tuple2<String, Integer> }}}).
  */
private class SelectEnglishAndTokenizeFlatMap extends FlatMapFunction[String, (String, Int)] {
  lazy val jsonParser = new ObjectMapper()

  override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
    val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
    val isEnglish = jsonNode.has("lang") && jsonNode.get("lang").asText() == "en"
    val hasText = jsonNode.has("text")

    (isEnglish, hasText, jsonNode) match {
      case (true, true, node) => {
        val tokenizer = new StringTokenizer(node.get("text").asText())

        while (tokenizer.hasMoreTokens) {
          val token = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase()
          if (token.nonEmpty && token.length >= 5) out.collect((token, 1))
        }
      }
      case _ =>
    }
  }

}
