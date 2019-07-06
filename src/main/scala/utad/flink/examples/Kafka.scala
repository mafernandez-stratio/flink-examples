package utad.flink.examples

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object Kafka extends App {

  val env =  StreamExecutionEnvironment.getExecutionEnvironment

  val p = new Properties()
  p.setProperty("bootstrap.servers", "127.0.0.1:9092")

  val kafkaData = env.addSource(new FlinkKafkaConsumer011("test", new SimpleStringSchema(), p))

  val words = kafkaData.flatMap(value => value.split("\\s+").map(value => (value,1))).keyBy(0).sum(1)

  words.print()

  env.execute("Kafka Example")
}
