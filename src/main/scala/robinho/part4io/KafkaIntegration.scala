package robinho.part4io

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

object KafkaIntegration {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // read simple data (strings) from a Kafka Topic
  // read + write custom data
  def readStrings(): Unit = {
    val kafkaSource = KafkaSource
      .builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("events")
      .setGroupId("events-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaStrings = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    kafkaStrings.print()
    env.execute()
  }

  // read custom data
  case class Person(name: String, age: Int)
  class PersonDeserializer extends DeserializationSchema[Person] {
    override def deserialize(message: Array[Byte]): Person = {
      // format: name,age
      val string = new String(message)
      val tokens = string.split(",")
      val name = tokens(0)
      val age = tokens(1)
      Person(name, age.toInt)
    }

    override def isEndOfStream(t: Person): Boolean = false

    override def getProducedType: TypeInformation[Person] =
      implicitly[TypeInformation[Person]]
  }

  def readCustomData(): Unit = {
    val kafkaSource = KafkaSource
      .builder[Person]()
      .setBootstrapServers("localhost:9092")
      .setTopics("people")
      .setGroupId("people-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new PersonDeserializer())
      .build()

    val kafkaPeople = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    kafkaPeople.print()
    env.execute()
  }

  // write custom data
  // need serializer

  class PersonSerializer(topic: String) extends KafkaRecordSerializationSchema[Person] {
    override def serialize(
        person: Person,
        context: KafkaRecordSerializationSchema.KafkaSinkContext,
        timestamp: lang.Long
    ): ProducerRecord[Array[Byte], Array[Byte]] = {
      new ProducerRecord(topic, null, s"${person.name},${person.age}".getBytes("UTF-8"))
    }
  }

  def writeCustomData(): Unit = {
    val kafkaSink = KafkaSink
      .builder[Person]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(new PersonSerializer("people"))
      .build()

    val peopleStream = env.fromElements(
      Person("Alice", 10),
      Person("Bob", 11),
      Person("Charlie", 12),
    )

    peopleStream.sinkTo(kafkaSink)

    peopleStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    // readStrings()
    // readCustomData()
    writeCustomData()
  }
}
