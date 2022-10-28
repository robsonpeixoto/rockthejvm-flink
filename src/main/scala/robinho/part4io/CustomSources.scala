package robinho.part4io

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSources {
  // source of numbers, randomly generated
  class RandomNumberGeneratorSource(minEventsPerSeconds: Double) extends SourceFunction[Long] {
    // create local fields/methods
    val maxSleepTime = (1000 / minEventsPerSeconds).toLong
    var isRunning = true

    // called ONCE, when the function is instantiated
    // runs on a single dedicated thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      while (isRunning) {
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)

        // push something to the output
        ctx.collect(nextNumber)
      }
    }

    // called at application shutdown
    // contract: the run method shoould stop immediately
    override def cancel(): Unit =
      isRunning = false
  }

  def demoSourceFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbersStream = env.addSource(new RandomNumberGeneratorSource(10))
    numbersStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoSourceFunction()
  }
}
