package robinho.part2datastreams

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._
import robinho.generators.shopping._

object Partitions {

  // splitting = partitioning
  def demoPartitioner(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val shoppingCartEvents: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s

    // partitioner = log to split the data
    val partitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        // hash code % number of partition ~ even distribution
        key.hashCode % numPartitions
      }
    }

    val partitionedStream = shoppingCartEvents.partitionCustom(partitioner, e => e.userId)

//    partitionedStream.print()

    /*
    Bad because
    - you lose parallelism
    - you risk overloading the task with the disproportionate data

    Good for e.g sending HTTP requests controlling the concurrency
     */
    val badPartitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        numPartitions - 1 // last partition index
      }
    }

    val badPartitionedStream = shoppingCartEvents
      .partitionCustom(badPartitioner, e => e.userId)
      .shuffle // redistribution of data evenly - involves data transfer through network (slow)
    badPartitionedStream.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }
}
