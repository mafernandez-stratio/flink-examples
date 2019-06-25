package utad.flink.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
  * Usage: WordCount --input <path> --output <path>
  */
object WordCount extends App {

  val params = ParameterTool.fromArgs(args)

  val env = ExecutionEnvironment.getExecutionEnvironment

  env.getConfig.setGlobalJobParameters(params)

  val text = env.readTextFile(params.get("input"))

  val counts = text.flatMap(_.toLowerCase.split("\\W+").filter(_.startsWith("j")))
    .map((_, 1))
    .groupBy(0)
    .sum(1)

  counts.writeAsCsv(params.get("output"), System.lineSeparator, ",")
  env.execute("WordCount Example")

}

