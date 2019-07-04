package utad.flink.examples

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.types.NullValue

import scala.collection.JavaConversions._

private final class InitVertices(srcId: String) extends MapFunction[String, Double] {
  override def map(id: String) = {
    if (id.equals(srcId)) {
      0.0
    } else {
      Double.MaxValue
    }
  }
}

private final class MinDistanceMessenger extends ScatterFunction[String, Double, Double, Double] {

  override def sendMessages(vertex: Vertex[String, Double]) {
    if (vertex.getValue < Double.PositiveInfinity) {
      for (edge: Edge[String, Double] <- getEdges) {
        sendMessageTo(edge.getTarget, vertex.getValue + edge.getValue)
      }
    }
  }
}

/**
  * Function that updates the value of a vertex by picking the minimum
  * distance from all incoming messages.
  */
private final class VertexDistanceUpdater extends GatherFunction[String, Double, Double] {

  override def updateVertex(vertex: Vertex[String, Double], inMessages: MessageIterator[Double]) {
    var minDistance = Double.MaxValue
    while (inMessages.hasNext) {
      val msg = inMessages.next
      if (msg < minDistance) {
        minDistance = msg
      }
    }
    if (vertex.getValue > minDistance) {
      setNewVertexValue(minDistance)
    }
  }
}

object GraphApp extends App {

  val user1Name = "George"

  val env = ExecutionEnvironment.getExecutionEnvironment

  val currentDirectory = new java.io.File(".").getCanonicalPath

  /* format: user, friend */
  val friends: DataSet[(String, String)] = env.readTextFile(s"$currentDirectory/src/main/resources/graph_data.txt").map{ line =>
    val words = line.split("\\s+")
    (words.head, words.last)
  }

  val edges: DataSet[Edge[String, NullValue]] = friends.map{ value =>
    val edge = new Edge[String, NullValue]()
    edge.setSource(value._1)
    edge.setTarget(value._2)
    edge
  }

  /* create graph from edges dataset */
  val friendsGraphs: Graph[String, NullValue, NullValue] = Graph.fromDataSet(edges, env)

  val weightedfriendsGraph: Graph[String, NullValue, Double] = friendsGraphs.mapEdges(_ => 1.0)

  val result: DataSet[Vertex[String, Double]] = weightedfriendsGraph.mapVertices{ vertex =>
    if(vertex.getId.equals(user1Name)){
      0.0
    } else {
      Double.PositiveInfinity
    }
  }.runScatterGatherIteration(new MinDistanceMessenger, new VertexDistanceUpdater, 10).getVertices

  /* get only friends of friends for George */
  val fOfUser1 = result.filter(_.getValue == 2)

  fOfUser1.print

}

