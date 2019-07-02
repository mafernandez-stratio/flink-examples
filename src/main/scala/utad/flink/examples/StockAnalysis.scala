package utad.flink.examples

import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object StockAnalysis extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  val uuid = UUID.randomUUID()

  val currentDirectory = new java.io.File(".").getCanonicalPath

  val data = env.readTextFile(s"$currentDirectory/src/main/resources/FUTURES_TRADES.txt").map{ trx =>
    val words = trx.split(",")
    // date, time, Name, trade, volume
    (words(0), words(1), "XYZ", words(2).toDouble, words(3).toInt)
  }.assignAscendingTimestamps{ event =>
    val sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    sdf.parse(s"${event._1} ${event._2}").getTime
  }

  // Compute per window statistics
  val change = data
    .keyBy(_._3)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(new TrackChange())

  change.writeAsText(s"/tmp/flink/stock/$uuid/report.txt")

  // Alert when price change from one window to another is more than threshold
  val largeDelta = data
    .keyBy(_._3)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(new TrackLargeDelta(5))

  largeDelta.writeAsText(s"/tmp/flink/stock/$uuid/alert.txt")

  env.execute("Stock Analysis")

}

class TrackChange extends ProcessWindowFunction[(String, String, String, Double, Int), String, String, TimeWindow] {

  private var prevWindowMaxTrade: ValueState[Double] = _
  private var prevWindowMaxVol: ValueState[Int] = _

  def process(key: String, context: Context, input: Iterable[(String, String, String, Double, Int)], out: Collector[String]) = {
    var windowStart = ""
    var windowEnd = ""
    var windowMaxTrade = 0.0              // 0
    var windowMinTrade = 0.0              // 106
    var windowMaxVol = 0
    var windowMinVol = 0                 // 348746

    input.foreach{ element =>
      if (windowStart.isEmpty()) {
        windowStart = s"${element._1}:${element._2}"   // 06/10/2010 : 08:00:00
        windowMinTrade = element._4
        windowMinVol = element._5
      }

      if (element._4 > windowMaxTrade)
        windowMaxTrade = element._4
      if (element._4 < windowMinTrade)
        windowMinTrade = element._4

      if (element._5 > windowMaxVol)
        windowMaxVol = element._5
      if (element._5 < windowMinVol)
        windowMinVol = element._5

      windowEnd = s"${element._1}:${element._2}"
    }

    var maxTradeChange = 0.0
    var maxVolChange = 0.0

    if (prevWindowMaxTrade.value() != 0) {
      maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100
    }

    if (prevWindowMaxVol.value() != 0)
      maxVolChange = ((windowMaxVol - prevWindowMaxVol.value())*1.0 / prevWindowMaxVol.value()) * 100

    out.collect(
      s"${windowStart} - ${windowEnd}, ${windowMaxTrade}, ${windowMinTrade}, - ${f"$maxTradeChange%.2f"}, ${windowMaxVol}, ${windowMinVol}, ${f"$maxVolChange%.2f"}"
    )

    prevWindowMaxTrade.update(windowMaxTrade)
    prevWindowMaxVol.update(windowMaxVol)
  }

  override def open(parameters: Configuration) ={
    prevWindowMaxTrade = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("prev_max_trade", createTypeInformation[Double], 0.0)
    )
    prevWindowMaxVol = getRuntimeContext.getState(
      new ValueStateDescriptor[Int]("prev_max_vol", createTypeInformation[Int], 0)
    )
  }

}

class TrackLargeDelta(threshold: Double) extends ProcessWindowFunction[(String, String, String, Double, Int), String, String, TimeWindow] {

  private var prevWindowMaxTrade: ValueState[Double] = _

  override def process(key: String, context: Context, input: Iterable[(String, String, String, Double, Int)], out: Collector[String]) = {
    val prevMax = prevWindowMaxTrade.value()
    var currMax = 0.0
    var currMaxTimeStamp = ""

    input.foreach{ element =>
      if (element._4 > currMax) {
        currMax = element._4
        currMaxTimeStamp = s"${element._1}:${element._2}}"
      }
    }

    // check if change is more than specified threshold
    val maxTradePriceChange = ((currMax - prevMax)/prevMax)*100

    if (prevMax != 0 &&  // don't calculate delta the first time
      Math.abs((currMax - prevMax)/prevMax)*100 > threshold){
      out.collect(
        s"Large Change Detected of ${f"$maxTradePriceChange%.2f"}% ($prevMax - $currMax) at $currMaxTimeStamp"
      )
    }
    prevWindowMaxTrade.update(currMax)
  }

  override def open(parameters: Configuration): Unit ={
    val descriptor =  new ValueStateDescriptor[Double]( "prev_max", createTypeInformation[Double], 0.0)
    prevWindowMaxTrade = getRuntimeContext().getState(descriptor)
  }
}

