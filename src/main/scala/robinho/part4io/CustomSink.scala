package robinho.part4io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{FileWriter, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.Scanner

object CustomSink {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stringStream = env.fromElements(
    "This is an example of sink function",
    "some other string",
    "Daniel says this is ok"
  )

  // pu the stirngs to a file sink

  class FileSink(path: String) extends RichSinkFunction[String] {
    var writer: PrintWriter = _
    // called once per event in the datastream
    override def invoke(event: String, ctx: SinkFunction.Context): Unit = {
      writer = new PrintWriter(new FileWriter(path, true))
      writer.println(event)
    }

    override def open(parameters: Configuration): Unit = {
      writer = new PrintWriter(new FileWriter(path, true))
    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
    }
  }

  def demoFileSink(): Unit = {
    stringStream.addSink(new FileSink("output/demoFileSink2.txt"))
    stringStream.print()
    env.execute()
  }

  /** Create s sink function that will push data (as strings) toa socket sink.
    */

  class SocketSink(host: String, port: Int) extends RichSinkFunction[String] {

    var socket: Socket = _
    var writer: PrintWriter = _

    override def invoke(element: String, ctx: SinkFunction.Context): Unit = {
      writer.println(element)
    }

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host, port)
      writer = new PrintWriter(socket.getOutputStream)


    }

    override def close(): Unit = {
      writer.flush()
      writer.close()
      socket.close()
    }


  }

  def demoSocketSink(): Unit = {
    stringStream.addSink(new SocketSink("localhost", 12345)).setParallelism(1)
    stringStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    // demoFileSink()
    demoSocketSink

  }
}

/*
- start data receiver
- start flink
 */

object DataReceiver {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(12345)
    println("Waiting for Flink to connect...")
    val socket = server.accept()
    val reader = new Scanner(socket.getInputStream)
    println("Flink connected. Reading...")

    while (reader.hasNextLine) {
      println(s"> ${reader.nextLine()}")
    }

    socket.close()
    println("All data read. Closing app.")
    server.close()
  }
}
