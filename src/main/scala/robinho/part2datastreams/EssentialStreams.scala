package robinho.part2datastreams

import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
    // 1 - execution environment
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    import org.apache.flink.streaming.api.scala._
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleNumberStream.print()

    // at the end
    env.execute
  }

  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
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

  def main(args: Array[String]): Unit = {
    demoTransformations()
  }

}
