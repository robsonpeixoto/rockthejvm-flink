package robinho.part2datastreams

import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import robinho.generators.shopping._

import java.time.Instant

object TimeBasedTransformations {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents: DataStream[ShoppingCartEvent] = env.addSource(
    new ShoppingCartEventsGenerator(
      sleepMillisPerEvent = 100,
      batchSize = 5,
      baseInstant = Instant.parse("2022-09-08T00:00:00.000Z")
    )
  )

  // 1. Event time = the moment the event was CREATED
  // 2. Processing time = the moment the event ARRIVES AT FLINK

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

  /*
  With event time
  - we NEED to care about handling late date - done with watermarks
  - we don't care about Flink internal time
  - we might see faster results
  - same events + different runs => same results
   */
  def demoEventTime(): Unit = {
    val groupedEventByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500 millis
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] = groupedEventByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  /** Custom watermarks
    */

  // with every new MAX timestamp, every new incoming element
  class BoundedOutOfOrdernessGenerator(maxDelay: Long)
      extends WatermarkGenerator[ShoppingCartEvent] {
    var currentMaxTimestamp: Long = 0L

    override def onEvent(
        event: ShoppingCartEvent,
        eventTimestamp: Long,
        output: WatermarkOutput
    ): Unit = {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp)
      // emitting a watermark is NOT mandatory
      // every subsequent event which is older than this particular timestamp will be discarded
      // output.emitWatermark(new Watermark(event.time.toEpochMilli))
    }

    // can also call onPeriodicEmit to MAYBE emit watermarks regularly - up to us to maybe emit a watermark at these times
    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
    }
  }

  def demoEventTimeWithCustomWatermarkGenerator(): Unit = {
    // control how often FLink calls onPeriodicEmit
    env.getConfig.setAutoWatermarkInterval(1000L) // call onPeriodicEmit every 1 second


    val groupedEventByWindow = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator(_ => new BoundedOutOfOrdernessGenerator(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

    def countEventsByWindow: DataStream[String] = groupedEventByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  /*
  Group by window, every 3s, tumbling (non-overlapping), PROCESSING TIME
  With processing time
  - we don't care when the event was created
  - multiple runs generate different results

  3> Window [TimeWindow{start=1662647991000, end=1662647994000}] 20
  4> Window [TimeWindow{start=1662647994000, end=1662647997000}] 30

  11> Window [TimeWindow{start=1662648030000, end=1662648033000}] 25
  12> Window [TimeWindow{start=1662648033000, end=1662648036000}] 30

   */

  def demoProcessingTime(): Unit = {
    def groupedEventByWindow =
      shoppingCartEvents.windowAll((TumblingProcessingTimeWindows.of(Time.seconds(3))))
    def countEventsByWindow: DataStream[String] = groupedEventByWindow.process(new CountByWindowAll)
    countEventsByWindow.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    // demoProcessingTime()
    // demoEventTime()
    demoEventTimeWithCustomWatermarkGenerator()
  }

}
