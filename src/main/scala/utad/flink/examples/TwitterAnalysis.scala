package utad.flink.examples

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.twitter.TwitterSource

object TwitterAnalysis extends App {

  val keywords = List("global warming","pollution","earth","temperature", "increase","weather", "change", "biosphere",
    "hydrosphere", "lithosphere", "solar radiation", "ice core", "fossil fuel", "depolarization", "emissions",
    "climate","co2","air quality","dust","carbondioxide","greenhouse","ozone","methane","sealevel","sea level")

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val twitterCredentials = new Properties()
  twitterCredentials.load(new FileInputStream("/Users/miguelangelfernandezdiaz/workspace/twitter.properties"))

  val streamSource: DataStream[String] = env.addSource(new TwitterSource(twitterCredentials))

  lazy val jsonParser = new ObjectMapper()

  val parsedData = streamSource.map{ value =>
    jsonParser.readValue(value, classOf[JsonNode])
  }

  val englishTweets = parsedData.filter{ node =>
    node.has("lang") && node.get("lang").asText() == "en"
  }

  val relevantTweets = englishTweets.filter{ tweet =>
    if(!tweet.has("text")){
      false
    } else {
      val text = tweet.get("text").asText.toLowerCase
      keywords.find(text.contains(_)).isDefined
    }
  }

  // Format: <source, tweetObject>
  val tweetsBySource = relevantTweets.map{ node =>
    if(node.has("source")){
      node.get("source").asText.toLowerCase match {
        case source if (source.contains("ipad") || source.contains("iphone")) =>
          ("AppleMobile", node)
        case source if source.contains("mac") =>
          ("AppleMac", node)
        case source if source.contains("android") =>
          ("Android", node)
        case source if source.contains("blackBerry") =>
          ("BlackBerry", node)
        case source if source.contains("web") =>
          ("Web", node)
        case _ =>
          ("Other", node)
      }
    } else {
      ("Unknown", node)
    }
  }

  val DATE_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"
  val dateFormat = new SimpleDateFormat(DATE_FORMAT)
  dateFormat.setLenient(true)

  // Format: <source, hourOfDay, 1>
  tweetsBySource.map{ tweet =>
    val createdAt = tweet._2.get("created_at").asText
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat.parse(createdAt))
    (tweet._1, s"${calendar.get(Calendar.HOUR_OF_DAY)}th hour", 1)
  }.keyBy(0,1) // groupBy source and hour
   .sum(2)     // sum for each category i.e. Number of tweets from 'source' in given 'hour'
   .print()
  // e.g. 100 tweets from Android about Pollution in 16th hour of day
  //      150 tweets from Apple devices about Pollution in 20th hour of day etc.

  env.execute("Twitter Analysis")
}
