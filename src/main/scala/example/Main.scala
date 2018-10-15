package example

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.rogach.scallop._
import play.api.libs.json.{JsObject, Json}

import scala.collection.JavaConversions._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val hosts = opt[List[String]](required = true, descr = "The cassandra database ip(s). Comma-separated, if there is more then one.")
  val keyspace = opt[String](required = true, descr = "The name of the keyspace")
  val table = opt[String](required = true, descr = "The name of the table")
  val port = opt[Int](default = Some(9042), descr = "Optional, the port, default value is 9042")
  val progress = opt[Boolean](default = Some(false), descr = "Print the progress to stderr, defaults to false")
  val fetchSize = opt[Int](default = Some(3000), descr = "The amount of rows which is retrieved simultaneously. Defaults to 3000")

  verify()
}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    for {
      hosts <- conf.hosts.get
      keyspace <- conf.keyspace.get
      table <- conf.table.get
      port <- conf.port.get
      progress <- conf.progress.get
      fetchSize <- conf.fetchSize.get
    } yield {
      if (progress) updateProgress("Connecting to cassandra.")

      val (cluster, session) = Cassandra(hosts, keyspace, port)
      session.execute(s"use $keyspace")

      if (progress) updateProgress(s"Connected to cassandra '${cluster.getClusterName}'")

      val start = System.currentTimeMillis()
      executeQuery(session, table, fetchSize, progress)
      if(progress) Console.err.println(s" \nTook ${(System.currentTimeMillis() - start) / 1000}s\n")
    }

    System.exit(0)
  }

  def executeQuery(session: Session, table: String, fetchSize: Int, progress: Boolean) = {
    val columnValues = (row: Row) => {
      row.getColumnDefinitions.iterator().map { definition =>
        Column(definition.getName, row.getString(definition.getName))
      }
    }

    val escapedString2Json = (s: String) => {
      Try {
        val unescaped = StringContext.processEscapes(s)
        Json.parse(unescaped.substring(1, unescaped.length - 1))
      }.toOption.getOrElse {
        Json.parse(s)
      }
    }

    val columns2Json = (columns: Iterator[Column]) => {
      columns.flatMap { case Column(name, value) =>
        Try(Json.parse(value)) match {
          case Success(json) =>

            val map = json.as[JsObject].fields.map { case (fieldName, fieldValue) =>
              fieldName -> escapedString2Json(fieldValue.toString)
            }

            Some(JsObject(map))
          case Failure(e) =>
            Console.err.println(s"Could not convert column '$name' to json ${e.getMessage}")
            None
        }
      }
    }

    if (progress) updateProgress("Execute query.")

    val statement = new SimpleStatement(s"select json * from $table;").setFetchSize(fetchSize)
    val rs = session.execute(statement)
    rs.toIterator.zipWithIndex.flatMap { case (row, index) =>
      if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) rs.fetchMoreResults()
      if (progress) updateProgress(s"$index rows.")
      columnValues.andThen(columns2Json)(row)
    }.foreach { json =>
      Console.println(Json.prettyPrint(json))
    }
  }

  def updateProgress(s: String) = {
    val max = 80
    val sliced = s.slice(0, max)
    val diff = max - sliced.length
    val output = sliced + (0 to diff).map(_ => " ").mkString
    Console.err.print(s"$output\r")
  }

  implicit def toScalaFuture(resultSet: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]
    Futures.addCallback[ResultSet](resultSet, new FutureCallback[ResultSet] {
      override def onSuccess(result: ResultSet): Unit = promise.success(result)
      override def onFailure(t: Throwable): Unit = promise.failure(t)
    })
    promise.future
  }

  case class Column(name: String, value: String)

}
