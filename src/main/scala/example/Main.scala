package example

import com.datastax.driver.core.{ResultSet, ResultSetFuture, Row, Session}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.rogach.scallop._
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val hosts = opt[List[String]](required = true)
  val keyspace = opt[String](required = true)
  val table = opt[String](required = true)
  val port = opt[Int](default = Some(9042))
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
    } yield {
      val (_, session) = Cassandra(hosts, keyspace, port)
      session.execute(s"use $keyspace")

      Await.result(executeQuery(session, table), Duration.Inf)
    }

    System.exit(0)
  }

  def executeQuery(session: Session, table: String) = {
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

    val prettyPrint = (values: List[JsValue]) => {
      values.map(Json.prettyPrint).foreach(println)
    }

    session.executeAsync(s"select json * from $table;").map { result =>
      result.toList.flatMap { row =>
        columnValues.andThen(columns2Json)(row)
      }
    }.map(prettyPrint)
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
