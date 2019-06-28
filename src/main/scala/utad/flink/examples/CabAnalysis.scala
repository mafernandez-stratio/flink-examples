package utad.flink.examples

import org.apache.flink.api.scala._

/**
  * Usage: CabAnalysis
  *
  * Using Flink run:
  *   mvn package
  *   flink-1.8.0/bin/flink run flink-examples/target/flink-examples-0.1-SNAPSHOT.jar
  */
object CabAnalysis extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment

  val currentDirectory = new java.io.File(".").getCanonicalPath

  val data = env.readTextFile(s"$currentDirectory/src/main/resources/cab-flink.txt").map { line =>
    val words = line.split(",")
    if(words(4).equalsIgnoreCase("yes")){
      (words(0), words(1), words(2), words(3), true, words(5), words(6), Integer.parseInt(words(7)))
    } else {
      (words(0), words(1), words(2), words(3), false, words(5), words(6), 0)
    }
  }.filter(_._5)

  // most popular destination
  val popularDest = data.map(trip => (trip._7, trip._8)).groupBy(0).sum(1).maxBy(1)
  popularDest.writeAsText(s"$currentDirectory/src/main/resources/cabs/popular_dest_batch.txt").setParallelism(1)

  // avg. passengers per trip source: place to pickup most passengers
  val avgPassPerTrip = data.map{ line =>
    (line._6,line._8, 1)
  }.groupBy(0).reduce {
    (left, right) => (left._1, left._2+right._2, left._3+right._3)
  }.map { w =>
    (w._1, w._2.toFloat/w._3)
  }

  avgPassPerTrip.writeAsText(s"$currentDirectory/src/main/resources/cabs/avg_passengers_per_trip.txt").setParallelism(1)

  // avg. passengers per driver: popular/efficient driver
  val avgPassPerDriver = data.map { line =>
    (line._4, line._8, 1)
  }.groupBy(0).reduce {
    (left, right) => (left._1, left._2+right._2, left._3+right._3)
  }.map{ w =>
    (w._1, w._2.toFloat/w._3)
  }

  avgPassPerDriver.writeAsText(s"$currentDirectory/src/main/resources/cabs/avg_passengers_per_driver.txt").setParallelism(1)

  env.execute("Cab Analysis")

}
