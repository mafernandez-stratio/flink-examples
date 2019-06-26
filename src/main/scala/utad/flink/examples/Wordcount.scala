package utad.flink.examples

import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
  * Usage: WordCount --input <path> --output <path>
  *
  * Using Flink run:
  *   mvn package
  *   flink-1.8.0/bin/flink run flink-examples/target/flink-examples-0.1-SNAPSHOT.jar --input flink-examples/src/main/resources/pg16961.txt --output /tmp/testflink/wordcount.out
  */
object WordCount extends App {

  val params = ParameterTool.fromArgs(args)

  val env = ExecutionEnvironment.getExecutionEnvironment

  env.getConfig.setGlobalJobParameters(params)

  val text: DataSet[String] = env.readTextFile(params.get("input"))

  val counts: AggregateDataSet[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+").filter(_.startsWith("j")))
    .map((_, 1))
    .groupBy(0)
    .sum(1)

  val sink: DataSink[(String, Int)] = counts.writeAsCsv(params.get("output"), System.lineSeparator, ",")
  env.execute("WordCount Example")

}

