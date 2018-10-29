package example

import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import JsonColumnParser._

import scala.io.Source
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

object Extr {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    for {
      hosts <- conf.hosts.toOption
      keyspace <- conf.keyspace.toOption
      table <- conf.table.toOption
      query <- conf.query.toOption
      port <- conf.port.toOption
      progress <- conf.progress.toOption
      fetchSize <- conf.fetchSize.toOption
      mode <- conf.mode.toOption
      consistencyLevel <- conf.consistencyLevel.toOption.map {
        case cl if cl == ConsistencyLevel.ANY.name() => ConsistencyLevel.ANY
        case cl if cl == ConsistencyLevel.ONE.name() => ConsistencyLevel.ONE
        case cl if cl == ConsistencyLevel.TWO.name() => ConsistencyLevel.TWO
        case cl if cl == ConsistencyLevel.THREE.name() => ConsistencyLevel.THREE
        case cl if cl == ConsistencyLevel.QUORUM.name() => ConsistencyLevel.QUORUM
        case cl if cl == ConsistencyLevel.ALL.name() => ConsistencyLevel.ALL
        case cl if cl == ConsistencyLevel.LOCAL_QUORUM.name() => ConsistencyLevel.LOCAL_QUORUM
        case cl if cl == ConsistencyLevel.EACH_QUORUM.name() => ConsistencyLevel.EACH_QUORUM
        case cl if cl == ConsistencyLevel.SERIAL.name() => ConsistencyLevel.SERIAL
        case cl if cl == ConsistencyLevel.LOCAL_SERIAL.name() => ConsistencyLevel.LOCAL_SERIAL
        case cl if cl == ConsistencyLevel.LOCAL_ONE.name() => ConsistencyLevel.LOCAL_ONE
      }
    } yield {
      if (progress) Output("Connecting to cassandra.")

      val session = Cassandra(hosts, keyspace, port, consistencyLevel, fetchSize)
      session.execute(s"use $keyspace")

      if (progress) Output(s"Connected to cassandra '${session.getCluster.getClusterName}'")

      val start = System.currentTimeMillis()

      Try {
        mode match {
          case "import" =>
            importer(session, table, progress)
          case "export" =>
            exporter(session, table, query, progress)
        }
      } match {
        case Success(_) =>
        case Failure(e) =>
          Console.err.println(s"\nError during '$mode': message: '${e.getMessage}'")
          System.exit(1)
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

  def exporter(session: Session, table: String, query: String, progress: Boolean) = {
    if (progress) Output("Execute query.")

    val statement = if(query.nonEmpty) {
      new SimpleStatement(s"select json * from $table $query;")
    } else {
      new SimpleStatement(s"select json * from $table;")
    }

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
