package robinho.part3state

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import robinho.generators.shopping._
object RichFunctions {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val numbersStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6)

  // pure FP
  val tenxNumbers: DataStream[Int] = numbersStream.map(_ * 10)

  // "explicit" map functions
  val tenxNumbers_v2 = numbersStream.map(new MapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })

  // Rich Map function
  val tenxNumbers_v3 = numbersStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
  })

  // Rich Map function + lifecycle methods
  val tenxNumbersWithLifecycle = numbersStream.map(new RichMapFunction[Int, Int] {
    override def map(value: Int): Int = value * 10
    // optional overrides: lifecycle methods open/close
    // called BEFORE data goes through
    override def open(parameters: Configuration): Unit = {
      println("Starting my work!!")
    }
    // invoked AFTER all the data
    override def close(): Unit = {
      println("Finishing my work...")
    }
  })

  // ProcessFunction - the most general function abstraction in Flink
  val tenxNumbersProcess: DataStream[Int] = numbersStream.process(new ProcessFunction[Int, Int] {
    override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
      out.collect(value * 10)
    }

    // can also override the lifecycle methods
    override def open(parameters: Configuration): Unit =
      println("Process function starting")

    override def close(): Unit =
      println("Closing process function")
  })

  /** Exercise: "explode" all purchase events to a single item
    *
    * [("boots", 2), ("iPhone", 1)] -> ["boots","boots", "iPhone"] ->
    */

  def exercise(): Unit = {
    val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartStream: DataStream[AddToShoppingCartEvent] = exerciseEnv
      .addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .map(_.asInstanceOf[AddToShoppingCartEvent])

    shoppingCartStream
      .process(new ProcessFunction[AddToShoppingCartEvent, String] {
        override def processElement(
            value: AddToShoppingCartEvent,
            ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context,
            out: Collector[String]
        ): Unit = {
          (1 to value.quantity).foreach(_ => out.collect(value.sku))
        }
      })
      .print()

    exerciseEnv.execute()
  }

  def main(args: Array[String]): Unit = {
    // tenxNumbersProcess.print()
    // env.execute()

    exercise()
  }

}
