package utad.flink.examples

import java.util.UUID

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowExample extends App {

  // set up the streaming execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val source = env.socketTextStream("localhost",9090)

  //word map

  val values = source.flatMap(value => value.split("\\s+")).map(value => (value,1))

  val keyValue = values.keyBy(0)

  //tumbling window : Calculate wordcount for each 15 seconds
  val tumblingWindow = keyValue.timeWindow(Time.seconds(10))
  // sliding window : Calculate wordcount for last 5 seconds
  val slidingWindow = keyValue.timeWindow(Time.seconds(10),Time.seconds(5))
  //count window : Calculate for every 5 records
  val countWindow = keyValue.countWindow(10)

  //tumblingWindow.sum(1).name("tumblingwindow").print()
  //slidingWindow.sum(1).name("slidingwindow").print()
  //countWindow.sum(1).name("count window").print()

  tumblingWindow.sum(1).name("count window").writeAsText(s"/tmp/test/${UUID.randomUUID()}").setParallelism(1)

  System.out.println()

  // execute program
  env.execute("Window")

}
