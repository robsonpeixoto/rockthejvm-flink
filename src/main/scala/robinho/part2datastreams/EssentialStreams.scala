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


  def main(args: Array[String]): Unit = {
    applicationTemplate()
  }

}
