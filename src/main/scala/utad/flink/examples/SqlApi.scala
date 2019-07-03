package utad.flink.examples

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource

case class Row1(month: String, sum: Int) {
  override def toString: String = s"$month,$sum"
}

object SqlApi extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = BatchTableEnvironment.create(env)

  val currentDirectory = new java.io.File(".").getCanonicalPath

  /* create table from csv */
  val tableSrc = CsvTableSource.builder()
    .path(s"$currentDirectory/src/main/resources/avg.txt")
    .fieldDelimiter(",")
    .field("date", Types.STRING)
    .field("month", Types.STRING)
    .field("category", Types.STRING)
    .field("product", Types.STRING)
    .field("profit", Types.INT)
    .build()

  tableEnv.registerTableSource("CatalogTable", tableSrc)

  val catalog = tableEnv.scan("CatalogTable")

  /* querying with SQL API */
  val sql = "SELECT `month`, SUM(profit) AS sum1 FROM CatalogTable WHERE category = 'Category5' GROUP BY `month` ORDER BY sum1"
  val order20 = tableEnv.sqlQuery(sql)

  val order20Set = tableEnv.toDataSet[Row1](order20)

  //order20Set.writeAsText(s"/tmp/flink/tableapi/${System.currentTimeMillis}")
  order20Set.print()

  //env.execute("State")

}
