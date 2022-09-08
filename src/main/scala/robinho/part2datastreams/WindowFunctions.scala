package robinho.part2datastreams

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.assigners.{
  EventTimeSessionWindows,
  GlobalWindows,
  SlidingEventTimeWindows,
  TumblingEventTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import robinho.generators.gaming._

import java.time.Instant
import scala.concurrent.duration._

case object WindowFunctions {
  // use-case: stream of events for a gaming session
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime: Instant = Instant.parse("2022-09-06T00:00:00.000Z")
  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player "Bob" registered 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.online(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness( // once you get a event with time T, you will NOT accept further events with time < T - 500ms
          java.time.Duration.ofMillis(500)
        )
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long =
            element.eventTime.toEpochMilli
        })
    )

  // how many player were registered every 3 seconds?
  // [0..3s] [3s...6s] [6s...9s]
  val threeSecondsTumblingWindow =
    eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    //                                                  ^ input  ^ output ^ window type
    override def apply(
        window: TimeWindow,
        input: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Windows [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] =
      threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()

    env.execute()
  }

//  alternative: process window function witch offers a much richer API (lower-level)
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    override def process(
        context: Context,
        elements: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Windows [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow_v2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] =
      threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()

    env.execute()
  }

  class CountByWindowAllV3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                               ^ input      ^ acc ^ output

    // start counting from 0
    override def createAccumulator(): Long = 0L

    // every element increases accumulator by 1
    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    // push a final output out of the final accumulator
    override def getResult(accumulator: Long): Long = accumulator

    // accum1 + accum2 = a bigger accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

  def demoCountByWindow_v3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] =
      threeSecondsTumblingWindow.aggregate(new CountByWindowAllV3)
    registrationsPerThreeSeconds.print()

    env.execute()
  }

  /** Keyed streams and window functions
    */
  // each element will be assigned to a "mini-stream" for its own key
  val streamByType: KeyedStream[ServerEvent, String] =
    eventStream.keyBy(e => e.getClass.getSimpleName)

  // for every key, we'll have a separate window allocation
  val threeSecondsTumblingWindowByType =
    streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(
        key: String,
        window: TimeWindow,
        input: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit =
      out.collect(s"$key: $window, ${input.size}")
  }

  // alternative: process function
  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindowByType.apply(new CountByWindow)
    finalStream.print()

    env.execute()
  }

  /** Sliding Windows
    */

  // how many player were registered every 3 seconds, UPDATED EVERY 1s?
  // [0s...3s] [1s...4s] [2s...5s] ...

  def demoSlidingAllWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)

    val slidingWindowsAll =
      eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

    // process the windowed stream with similar window functions
    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)

    // similar to the other example
    registrationCountByWindow.print()
    env.execute()
  }

  /** Session windows = groups of events with NO MORE THAN a certain time gap in between them
    */

  // how many registration events do we have NO MORE THAN 1s apart

  def demoSessionWindows(): Unit = {
    val groupBySessionWindows =
      eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))

    // operate any kind of window function
    val countBySessionWindows = groupBySessionWindows.apply(new CountByWindowAll)

    // same things as before
    countBySessionWindows.print()
    env.execute()
  }

  /** Global windows
    */
  // how many registration events do we have every 10 events

  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    //                                                  ^ input  ^ output ^ window type
    override def apply(
        window: GlobalWindow,
        input: Iterable[ServerEvent],
        out: Collector[String]
    ): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Windows [$window] $registrationEventCount")
    }
  }

  def demoGlobalWindow(): Unit = {
    val globalWindowsEvents =
      eventStream
        .windowAll(GlobalWindows.create())
        .trigger(CountTrigger.of[GlobalWindow](10))
        .apply(new CountByGlobalWindowAll)

    globalWindowsEvents.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    // demoCountByWindow()
    // demoCountByWindow_v2()
    // demoCountByWindow_v3()
    // demoCountByTypeByWindow()
    // demoSlidingAllWindows()
    demoGlobalWindow()
  }
}
