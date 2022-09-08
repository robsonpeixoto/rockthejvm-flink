package robinho.part2datastreams

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import robinho.generators.shopping._

object Triggers {

  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(
        context: Context,
        elements: Iterable[ShoppingCartEvent],
        out: Collector[String]
    ): Unit = {
      val window = context.window
      out.collect(s"Window [$window] ${elements.size}")
    }
  }

  // Triggers -> WHEN a window function is executed

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def demoFirstTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events/second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(CountTrigger.of[TimeWindow](5)) // the window function runs every 5 elements
      .process(new CountByWindowAll) // runs twice for the same windows because (10/5 = 2)

    shoppingCartEvents.print
    env.execute()
  }
  /*
  12> Window [TimeWindow{start=1662660400000, end=1662660405000}] 10
  1> Window [TimeWindow{start=1662660405000, end=1662660410000}] 10

  with trigger:
  5> Window [TimeWindow{start=1662660355000, end=1662660360000}] 5 <- trigger euning on the window 55000-60000 for the first time
  6> Window [TimeWindow{start=1662660355000, end=1662660360000}] 10 <- second trigger FOR THE SAME WINDOW
  7> Window [TimeWindow{start=1662660360000, end=1662660365000}] 5
  8> Window [TimeWindow{start=1662660360000, end=1662660365000}] 10

   */

  def demoPurgingTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // 2 events/second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events/window
      .trigger(
        PurgingTrigger.of(CountTrigger.of[TimeWindow](5))
      ) // the window function runs every 5 elements, THEN CLEARS THE WINDOW
      .process(new CountByWindowAll) // runs twice for the same windows because (10/5 = 2)

    shoppingCartEvents.print
    env.execute()
  }

  /*
  with purging trigger
  11> Window [TimeWindow{start=1662660810000, end=1662660815000}] 5
  12> Window [TimeWindow{start=1662660810000, end=1662660815000}] 5
  1> Window [TimeWindow{start=1662660815000, end=1662660820000}] 5
  2> Window [TimeWindow{start=1662660815000, end=1662660820000}] 5
  3> Window [TimeWindow{start=1662660820000, end=1662660825000}] 5
   */

  /*
  Other triggers:
  - EventTimeTrigger - happens by default when the watermark is > window end time (automatic for event time windows)
  - ProcessingTimeTrigger - fires when the current system time > window end time (automatic for processing time windows)
  - custom trigger - powerful APIs for custom firing behavior
   */
  def main(args: Array[String]): Unit = {
    // demoFirstTrigger()
    demoPurgingTrigger()
  }
}
