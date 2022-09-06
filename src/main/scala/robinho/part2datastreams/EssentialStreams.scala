package robinho.part2datastreams

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
    // 1 - execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    import org.apache.flink.streaming.api.scala._
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleNumberStream.print()

    // at the end
    env.execute
  }

  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    // is possible to configure parallelism globally
    // println(s"Current parallelism: ${env.getParallelism}")
    // env.setParallelism(2)
    // println(s"Current parallelism: ${env.getParallelism}")

    // map
    val doubledNumbers = numbers.map(_ * 2)

    // flatMap
    val expandedNumbers = numbers.flatMap(n => List(n, n + 1))

    // filter
    val filteredNumbers = numbers.filter(_ % 2 == 0)

    // it will create a file for each CPU(parallelism)
    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt")
    finalData.setParallelism(1)

    env.execute()
  }

  /** Exercise: FizzBuzz on Flink
    *   - take a stream of 100 natural numbers
    *   - for every number
    *     - if n % 3 == 0 then return "fizz"
    *     - if n % 5 == 0 => "buzz"
    *     - if both => "fizzbuzz"
    *   - write the numbers for which you said "fizzbuzz" to a file
    */
  case class FizzBuzzResult(n: Long, output: String)
  def fizzbuzz(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(1, 100)

    val fizzbuzz = numbers
      .map { (x: Long) =>
        x match {
          case n if n % 3 == 0 && n % 5 == 0 => FizzBuzzResult(n, "fizzbuzz")
          case n if n % 3 == 0               => FizzBuzzResult(n, "fizzbuzz")
          case n if n % 5 == 0               => FizzBuzzResult(n, "fizzbuzz")
          case n                             => FizzBuzzResult(n, s"$n")
        }
      }
      .filter(_.output == "fizzbuzz")
      .map(_.n)

    // add sink
    fizzbuzz
      .addSink(
        StreamingFileSink
          .forRowFormat(
            new Path("output/fizzbuzz_sink"),
            new SimpleStringEncoder[Long]("UTF-8")
          )
          .build()
      )
      .setParallelism(1)

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    fizzbuzz()
  }

}
