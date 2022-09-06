package robinho.part2datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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

  def demoExplicitTransformations(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(1, 100)

    // map
    val doubledNumbers = numbers.map(_ * 2)

    // map - explicit version
    val doubledNumbers_v2 = numbers.map(new MapFunction[Long, Long] {
      // declare fields, methods, ...
      override def map(value: Long): Long = value * 2
    })

    // flatMap
    val expandedNumbers = numbers.flatMap(n => Range.Long(1, n, 1).toList)

    // flatMap - explicit version
    val expandedNumbers_v2 = numbers.flatMap(new FlatMapFunction[Long, Long] {
      override def flatMap(n: Long, out: Collector[Long]): Unit =
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) // imperative style - pushes the new element downstream
        }
    })

    // process method
    // ProcessFunction is THE MOST GENERAL function to process elements in Flink
    val expandedNumbers_v3 = numbers.process(new ProcessFunction[Long, Long] {
      override def processElement(
          n: Long,
          context: ProcessFunction[Long, Long]#Context,
          out: Collector[Long]
      ): Unit =
        Range.Long(1, n, 1).foreach { i =>
          out.collect(i) // imperative style - pushes the new element downstream
        }
    })

    /*
     reduce
     happens on keyed streams

     Flink will create 2 tasks, and each task is a stream:
     - one for the key True
     - one for the key False
     */
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)

    // reduce - FP approach
    val sumByKey = keyedNumbers.reduce(_ + _) // sum up all the elements BY KEY

    // reduce - explicit approach
    val sumByKey_v2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      override def reduce(x: Long, y: Long): Long = x + y
    })

    sumByKey_v2.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoExplicitTransformations()
  }
}
