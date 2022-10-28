package robinho.part4io

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object CassandraIntegration {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int)

  // Create Keyspace and Table
  // create keyspace if not exists rtjvm with replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
  // create table if not exists rtjvm.people(name text, age int, primary key(name));
  // SELECT * FROM rtjvm.people;

  // write data to Cassandra
  def demoWriteDataToCassandra(): Unit = {
    val people = env.fromElements(
      Person("Daniel", 99),
      Person("Alice", 12),
      Person("Julie", 14),
      Person("Mon", 54)
    )

    // we can only write TUPLES to Cassandra
    val personTuples: DataStream[(String, Int)] = people.map(p => (p.name, p.age))

    CassandraSink
      .addSink(personTuples)
      .setQuery("INSERT INTO rtjvm.people(name, age) VALUES(?, ?)")
      .setHost("localhost")
      .build()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoWriteDataToCassandra
  }
}
