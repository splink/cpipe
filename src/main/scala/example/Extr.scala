package example

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import JsonColumnParser._

import scala.io.Source
import scala.language.implicitConversions

object Extr {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    for {
      hosts <- conf.hosts.toOption
      keyspace <- conf.keyspace.toOption
      table <- conf.table.toOption
      port <- conf.port.toOption
      progress <- conf.progress.toOption
      fetchSize <- conf.fetchSize.toOption
      mode <- conf.mode.toOption
    } yield {
      if (progress) Output("Connecting to cassandra.")

      val session = Cassandra(hosts, keyspace, port)
      session.execute(s"use $keyspace")

      if (progress) Output(s"Connected to cassandra '${session.getCluster.getClusterName}'")

      val start = System.currentTimeMillis()

      mode match {
        case "import" =>
          importer(session, table, progress)
        case "export" =>
          exporter(session, table, fetchSize, progress)
      }

      if (progress) Console.err.println(s"\nTook ${(System.currentTimeMillis() - start) / 1000}s")
    }

    System.exit(0)
  }

  def importer(session: Session, table: String, progress: Boolean) = {
    var index = 0
    val frame = new JsonFrame()
    Source.stdin.getLines().foreach { line =>
      frame.push(line.toCharArray).foreach { result =>
        string2JsObject(result).map { json =>
          index += 1
          if (progress) Output(s"$index rows.")
          session.execute(json2Query(json, table))
        }
      }
    }
  }

  def exporter(session: Session, table: String, fetchSize: Int, progress: Boolean) = {
    if (progress) Output("Execute query.")

    val statement = new SimpleStatement(s"select json * from $table;")

    val rs = session.execute(statement)
    rs.iterator().asScala.zipWithIndex.flatMap { case (row, index) =>
      if (rs.getAvailableWithoutFetching < statement.getFetchSize / 2 && !rs.isFullyFetched) {
        if (progress) Output(s"Got $index rows, off to get more...")
        rs.fetchMoreResults()
      }
      if (progress) Output(s"$index rows.")
      columnValues.andThen(columns2Json)(row)
    }.foreach { json =>
      Console.println(Json.prettyPrint(json))
    }
  }

  implicit def toScalaFuture(resultSet: ResultSetFuture): Future[ResultSet] = {
    val promise = Promise[ResultSet]
    Futures.addCallback[ResultSet](resultSet, new FutureCallback[ResultSet] {
      override def onSuccess(result: ResultSet): Unit = promise.success(result)

      override def onFailure(t: Throwable): Unit = promise.failure(t)
    })
    promise.future
  }

}
