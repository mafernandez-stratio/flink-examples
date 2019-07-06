package utad.flink.examples

import java.text.SimpleDateFormat

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

object AustinTrips extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)

  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val tableEnv = StreamTableEnvironment.create(env)

  val currentDirectory = new java.io.File(".").getCanonicalPath

  val rawTrips = CsvTableSource
    .builder()
    .path(s"$currentDirectory/src/main/resources/Austin_B-Cycle_Trips_clean.csv")
    .fieldDelimiter(",")
    .ignoreFirstLine()
    .field("TripID", Types.STRING)
    .field("MembershipType", Types.STRING)
    .field("BicycleID", Types.STRING)
    .field("CheckoutDate", Types.STRING)
    .field("CheckoutTime", Types.STRING)
    .field("CheckoutKioskID", Types.STRING)
    .field("CheckoutKiosk", Types.STRING)
    .field("ReturnKioskID", Types.STRING)
    .field("ReturnKiosk", Types.STRING)
    .field("TripDurationMinutes", Types.INT)
    .field("Month", Types.INT)
    .field("Year", Types.INT)
    .ignoreParseErrors()
    .build()

  tableEnv.registerTableSource("trips", rawTrips)

  val table = tableEnv.sqlQuery("SELECT * FROM trips")

  val stream = tableEnv.toAppendStream[Trip](table)
    .assignAscendingTimestamps{ trip =>
    val sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    //Format: 10/26/2014,13:12:00
    sdf.parse(s"${trip.CheckoutDate} ${trip.CheckoutTime}").getTime
  }

  val result1 = stream.filter{ trip =>
    val hour = trip.CheckoutTime.split(":").head.toInt
    hour >= 0 && hour <= 7
  }.keyBy("MembershipType")
    .window(TumblingEventTimeWindows.of(Time.days(7)))
    .max("TripDurationMinutes")


  val stringResult1 = result1.map(_.toString)

  stringResult1.addSink(new FlinkKafkaProducer011[String](
    "localhost:9092",
    "austintrips",
    new SimpleStringSchema)
  )
  result1.print("ResultByMembership")

  val result2 = stream.filter{ trip =>
    val hour = trip.CheckoutTime.split(":").head.toInt
    hour >= 0 && hour <= 7
  }.windowAll(TumblingEventTimeWindows.of(Time.days(7))).reduce{ (leftTrip, rightTrip) =>
    if(leftTrip.TripDurationMinutes > rightTrip.TripDurationMinutes){
      leftTrip
    } else {
      rightTrip
    }
  }

  val stringResult2 = result2.map(_.toString)

  stringResult2.addSink(new BucketingSink[String]("hdfs://localhost:9000/flink/austintrips"))

  result2.print("ResultGlobal")

  env.execute("Austin trips")

}

case class Trip(TripID: String,
                MembershipType: String,
                BicycleID: String,
                CheckoutDate: String,
                CheckoutTime: String,
                CheckoutKioskID: String,
                CheckoutKiosk: String,
                ReturnKioskID: String,
                ReturnKiosk: String,
                TripDurationMinutes: Int,
                Month: Int,
                Year: Int)
