package robinho.part3state

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import robinho.generators.shopping._

object KeyedState {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val shoppingCartEvents: DataStream[ShoppingCartEvent] = env
    .addSource(
      new SingleShoppingCartEventsGenerator(
        sleepMillisBetweenEvents = 100, // ~10 events/s
        generateRemoved = true
      )
    )

  /*
    How many events PER USER have been generated?
   */

  val eventsPerUser: KeyedStream[ShoppingCartEvent, String] = shoppingCartEvents.keyBy(_.userId)

  val numEventsPerUserNaive = eventsPerUser.process(
    new KeyedProcessFunction[String, ShoppingCartEvent, String] {
      var nEventsForThisUser = 0
      override def processElement(
          value: ShoppingCartEvent,
          ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
          out: Collector[String]
      ): Unit = { // instantiated ONCE PER KEY
        nEventsForThisUser += 1
        out.collect(s"User ${value.userId} - $nEventsForThisUser")
      }
    }
  )

  /*
    Problems with local vars:
    - they are local, so others nodes don't see them
    - if a node crashes, the var disappears
   */

  def demoValueState() = {
    val numEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        // can call .value to get current state
        // can call .update(newValue) to overwrite the current value
        var stateCounter: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // initialize all state
          stateCounter = getRuntimeContext // from RichFunction
            .getState(new ValueStateDescriptor[Long]("events-counter", classOf[Long]))
        }

        override def processElement(
            value: ShoppingCartEvent,
            ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
            out: Collector[String]
        ): Unit = { // instantiated ONCE PER KEY
          val nEventsForThisUser = stateCounter.value()
          stateCounter.update(nEventsForThisUser + 1)
          out.collect(s"User ${value.userId} - ${nEventsForThisUser + 1}")
        }
      }
    )
    numEventsPerUserStream.print()
    env.execute()
  }

  def demoListState() = {
    // store all the events per user id
    val numEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        var stateEventsForUser: ListState[ShoppingCartEvent] = _
        // needed to be careful to keep the size of the list BOUNDED
        // to remove a element from the List the better solution is to use the `.update` method to replace the List to a new one.

        override def open(parameters: Configuration): Unit = {
          // initialize all state
          stateEventsForUser = getRuntimeContext.getListState(
            new ListStateDescriptor[ShoppingCartEvent]("events-list", classOf[ShoppingCartEvent])
          )
        }

        override def processElement(
            value: ShoppingCartEvent,
            ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
            out: Collector[String]
        ): Unit = { // instantiated ONCE PER KEY
          stateEventsForUser.add(value)
          // import the Scala converters for collections
          import scala.collection.JavaConverters._ // implicit converters (extension methods)
          out.collect(s"User ${value.userId} - ${stateEventsForUser.get().asScala.size}")
        }
      }
    )
    numEventsPerUserStream.print()
    env.execute()

  }

  // MapState
  def demoMapState(): Unit = {
    val numEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        import scala.collection.JavaConverters._ // implicit converters (extension methods)
        var stateCountsPerEventType: MapState[String, Long] = _ // every `userId` will have a state

        override def open(parameters: Configuration): Unit = {
          stateCountsPerEventType = getRuntimeContext.getMapState(
            new MapStateDescriptor[String, Long](
              "events-map",
              classOf[String],
              classOf[Long]
            )
          )
        }

        override def processElement(
            value: ShoppingCartEvent,
            ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
            out: Collector[String]
        ): Unit = { // instantiated ONCE PER KEY
          // fetch the type of the event
          val eventType = value.getClass.getSimpleName
          // updating the sate

          if (stateCountsPerEventType.contains(eventType)) {
            val oldCount = stateCountsPerEventType.get(eventType)
            stateCountsPerEventType.put(eventType, oldCount + 1)
          } else {
            stateCountsPerEventType.put(eventType, 1)
          }

          // import the Scala converters for collections // implicit converters (extension methods)
          out.collect(s"${ctx.getCurrentKey} - ${stateCountsPerEventType.entries().asScala.mkString(",")}")
        }
      }
    )
    numEventsPerUserStream.print()
    env.execute()

  }

  // clear the state manually
  // clear the state at a regular interval
  def demoBoundedListState() = {
    // store all the events per user id
    val numEventsPerUserStream = eventsPerUser.process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        import scala.collection.JavaConverters._ // implicit converters (extension methods)

        var stateEventsForUser: ListState[ShoppingCartEvent] = _
        // needed to be careful to keep the size of the list BOUNDED
        // to remove a element from the List the better solution is to use the `.update` method to replace the List to a new one.

        // initialization of the state
        override def open(parameters: Configuration): Unit = {
          val descriptor = new ListStateDescriptor[ShoppingCartEvent]("events-list", classOf[ShoppingCartEvent])
          // configure the TTL of the state
          descriptor.enableTimeToLive(
            StateTtlConfig
              .newBuilder(Time.hours(1)) // clears the state after 1h
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
              .build()
          )
          // initialize all state
          stateEventsForUser = getRuntimeContext.getListState(descriptor)
        }

        override def processElement(
            value: ShoppingCartEvent,
            ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
            out: Collector[String]
        ): Unit = { // instantiated ONCE PER KEY

          stateEventsForUser.add(value)
          val currentEvents = stateEventsForUser.get().asScala.toList
          // manually clear the state
          // if (currentEvents.size > 10)
          //   stateEventsForUser.clear() // clearing is not done immediately

          // import the Scala converters for collections
          out.collect(s"User ${value.userId} - ${stateEventsForUser.get().asScala.size}")
        }
      }
    )
    numEventsPerUserStream.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    // demoValueState()
    // demoListState()
    // demoMapState
    demoBoundedListState
  }
}
