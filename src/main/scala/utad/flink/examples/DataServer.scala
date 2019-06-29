package utad.flink.examples

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random

object DataServer extends App {
  val listener = new ServerSocket(9090)
  try{
    val socket = listener.accept()
    System.out.println("Got new connection: " + socket.toString())
    try {
      val out = new PrintWriter(socket.getOutputStream(), true)
      val rand = new Random()
      while (true){
        val s = s"text${rand.nextInt(3)} text${rand.nextInt(3)} text${rand.nextInt(3)} text${rand.nextInt(3)} text${rand.nextInt(3)} text${rand.nextInt(3)}"
        System.out.println(s)
        out.println(s)
        Thread.sleep(rand.nextInt(5000))
      }
    } finally{
      socket.close()
    }
  } catch{
    case e: Exception => e.printStackTrace()
      e.printStackTrace()
  } finally{
    listener.close()
  }
}
