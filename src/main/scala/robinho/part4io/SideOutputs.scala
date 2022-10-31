package robinho.part4io

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import robinho.generators.shopping._

object SideOutputs {

  // shopping cart events
  // process thin in 2 different ways with the same function
  // e.g. events for user "Alice" , and all the events of everyone else

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents: DataStream[ShoppingCartEvent] = env
    .addSource(
      new SingleShoppingCartEventsGenerator(
        sleepMillisBetweenEvents = 100, // ~10 events/s
        generateRemoved = false
      )
    )

  // output tags - only available for ProcessFunctions
  val aliceTag = new OutputTag[ShoppingCartEvent]("alice-events") // name should be unique

  class AliceEventsFunction extends ProcessFunction[ShoppingCartEvent, ShoppingCartEvent] {
    override def processElement(
        event: ShoppingCartEvent,
        ctx: ProcessFunction[ShoppingCartEvent, ShoppingCartEvent]#Context,
        out: Collector[ShoppingCartEvent] // "primary" destination
    ): Unit =  {
      if (event.userId == "Alice"){
        ctx.output(aliceTag, event) // collection an event through a secondary destination
      } else {
        out.collect(event)
      }

    }
  }

  def demoSideOutput(): Unit = {
    val allEventButAlices = shoppingCartEvents.process(new AliceEventsFunction)
    val alicesEvents = allEventButAlices.getSideOutput(aliceTag)

    // process the datastreams separately
    alicesEvents.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoSideOutput()
  }

}
