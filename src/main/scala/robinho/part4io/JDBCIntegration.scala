package robinho.part4io

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

object JDBCIntegration {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int)

  // write data to JDBC
  def demoWriteToJDCB(): Unit = {
    val people = env.fromElements(
      Person("Daniel", 99),
      Person("Alice", 1),
      Person("Bob", 10),
      Person("Mary Jane", 43)
    )

    val jdbcSink = JdbcSink.sink[Person](
      // 1 - SQL statement
      "INSERT INTO PEOPLE (name, age) values (?, ?)",
      // 2 - the way to expand the wildcards with actual values
      new JdbcStatementBuilder[Person] {
        override def accept(statement: PreparedStatement, person: Person): Unit = {
          statement.setString(1, person.name) // the first ? is replaced with person.name
          statement.setInt(2, person.age) // the second ? is replaced with person.age
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:postgresql://localhost:5432/rtjvm")
        .withDriverName(classOf[org.postgresql.Driver].getName)
        .withUsername("docker")
        .withPassword("docker")
        .build()
    )

    // push the data through the sink
    people.addSink(jdbcSink)
    people.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoWriteToJDCB()
  }

}
