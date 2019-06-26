package utad.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Usage: StreamingWordCount --input <path> --output <path>
  */
object StreamingWordCount extends App {

  val params = ParameterTool.fromArgs(args)

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.getConfig.setGlobalJobParameters(params)

  val text: DataStream[String] = env.readTextFile(params.get("input"))

  val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split("\\W+").filter(_.startsWith("j")))
    .map((_, 1))
    .keyBy(0)
    .sum(1)

  val sink: DataStreamSink[(String, Int)] = counts.writeAsCsv(params.get("output"), FileSystem.WriteMode.NO_OVERWRITE, System.lineSeparator, ",")
  env.execute("Streaming WordCount Example")

}

